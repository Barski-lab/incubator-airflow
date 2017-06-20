#
# CWLStepOperator is required for CWLDAG
#   CWLStepOperator execute stage expects input job from xcom_pull
#   and post output by xcom_push

import schema_salad.ref_resolver
import cwltool.main
import cwltool.load_tool
from  cwltool.workflow import defaultMakeTool
import cwltool.errors
import logging
from cwltool.resolver import tool_resolver
from airflow.models import BaseOperator
from airflow.utils import (apply_defaults)
from jsonmerge import merge
import json
import os
import copy
from airflow.cwl_runner.cwlutils import shortname, flatten
import tempfile
import ruamel.yaml as yaml
import cwltool.stdfsaccess
import sys
import schema_salad.validate as validate

class CWLStepOperator(BaseOperator):

    ui_color = '#3E53B7'
    ui_fgcolor = '#FFF'

    @apply_defaults
    def __init__(
            self,
            cwl_step,
            ui_color=None,
            op_args=None,
            op_kwargs=None,
            *args, **kwargs):

        self.outdir = None

        self.cwl_step = cwl_step
        step_id = shortname(cwl_step.tool["id"]).split("/")[-1]

        super(self.__class__, self).__init__(task_id=step_id, *args, **kwargs)

        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}

        if ui_color:
            self.ui_color = ui_color

    def execute(self, context):

        logging.info('{0}: Running workflow: \n{1}'.format(self.task_id,
                                                               json.dumps(self.dag.cwlwf.tool, indent=4)))
        logging.info('{0}: Running step: \n{1}'.format(self.task_id,
                                                               json.dumps(self.cwl_step.tool, indent=4)))
        logging.info('{0}: Running embedded tool: \n{1}'.format(self.task_id, json.dumps(self.cwl_step.embedded_tool.tool, indent=4)))

        upstream_task_ids = [t.task_id for t in self.upstream_list]
        upstream_data = self.xcom_pull(context=context, task_ids=upstream_task_ids)

        logging.info('{0}: Collecting outputs from: \n{1}'.format(self.task_id, json.dumps(upstream_task_ids, indent=4)))

        promises = {}
        for j in upstream_data:
            data = j
            promises = merge(promises, data["promises"])
            if "outdir" in data:
                self.outdir = data["outdir"]

        if not self.outdir:
            raise cwltool.errors.WorkflowException("Outdir is not provided, please use job dispatcher")

        logging.info(
            '{0}: Upstream data: \n {1}'.format(self.task_id, json.dumps(upstream_data,indent=4)))

        logging.info(
            '{0}: Step inputs: \n {1}'.format(self.task_id, json.dumps(self.cwl_step.tool["inputs"],indent=4)))

        logging.info(
            '{0}: Step outputs: \n {1}'.format(self.task_id, json.dumps(self.cwl_step.tool["outputs"],indent=4)))

        jobobj = {}

        # for inp in self.cwl_step.tool["inputs"]:   # Maybe we can simplify it to get only by id
        #     jobobj_id = shortname(inp["id"]).split("/")[-1]
        #     if "source" in inp or "valueFrom" in inp:
        #         source_ids = []
        #         promises_outputs = []
        #         try:
        #             source_ids = [shortname(source) for source in inp["source"]] if isinstance(inp["source"], list) else [shortname(inp["source"])]
        #             promises_outputs = [promises[source_id] for source_id in source_ids if source_id in promises]
        #         except Exception as ex:
        #             logging.info("{0}: Couldn't find source field in step input:\n{1}".format(self.task_id,json.dumps(inp,indent=4)))
        #         logging.info('{0}: For source_ids: \n{1} \nfound upstream outputs: \n{2}'.format(self.task_id, source_ids, promises_outputs))
        #         if len(promises_outputs) > 1:
        #             if inp.get("linkMerge", "merge_nested") == "merge_flattened":
        #                 promises_outputs = flatten (promises_outputs)
        #         elif len(promises_outputs) == 1:
        #             promises_outputs = promises_outputs[0]
        #         elif "valueFrom" in inp:
        #             promises_outputs = None
        #         else:
        #             continue
        #         jobobj[jobobj_id] = promises_outputs
        #     elif "default" in inp:
        #         d = copy.copy(inp["default"])
        #         jobobj[jobobj_id] = d

        jobobj = promises

        logging.info('{0}: Collected job object: \n {1}'.format(self.task_id, json.dumps(jobobj,indent=4)))

        kwargs = self.dag.default_args
        kwargs['outdir'] = tempfile.mkdtemp(prefix=os.path.join(self.outdir, "task_tmp"))


        def get_type_dy_id (embedded_tool, id):
            print "Trying to get type by id ", id
            id_types = {shortname(tool_input["id"]).split("/")[-1]: tool_input["type"] for tool_input in embedded_tool["inputs"]}
            print "Here all of the input types for embedded tool\n", json.dumps(id_types, indent=4)
            print "Correspondent type is ", id_types[shortname(id).split("/")[-1]]
            return id_types[shortname(id).split("/")[-1]]


        def get_id_by_source (step_in, target_source):
            print "Trying to find id for ", shortname(target_source)
            for step_in_item in step_in:
                print "Looking in\n", json.dumps(step_in_item, indent=4)
                step_in_item_sources = step_in_item["source"] if isinstance(step_in_item["source"], list) else [step_in_item["source"]]
                print "Here we have sources\n", json.dumps([shortname(source) for source in flatten(step_in_item_sources)], indent=4)
                if shortname(target_source) in [shortname(source) for source in flatten(step_in_item_sources)]:
                    print "Source {} is found".format(shortname(target_source))
                    print "Correspondent id ", shortname(step_in_item["id"]).split("/")[-1]
                    return shortname(step_in_item["id"]).split("/")[-1]
                else:
                    print "Source is not found"


        def remove_not_provided_inputs_from_step (step, jobobj):
            for step_input in step["in"]:   # Check do we have valueFrom and default in in or in inputs
                if "valueFrom" in step_input or "default" in step_input or (
                        "source" in step_input and isinstance(step_input["source"], list)) or (
                        "source" in step_input and shortname(step_input["source"]) in jobobj):
                    continue
                else:
                    step["in"].remove(step_input)


        def get_inputs (step):
            inputs = [
                        {
                            "id": shortname(source),
                            "type": [
                                        "null",
                                        "Any",
                                        get_type_dy_id(step.embedded_tool.tool, get_id_by_source(step.tool["in"], source))
                                    ]
                        }
                        for source in [shortname(source) for source in
                                            list(set(flatten([step_in["source"] for step_in in step.tool["in"] if "source" in step_in])))
                                      ]
                    ]
            return inputs


        def get_outputs (step):
            return [{"id": shortname(step_out).split("/")[-1], "outputSource": shortname(step_out), "type": "Any"} for step_out in step.tool["out"]]


        def get_custom_step(workflow, step):
            target_step = [workflow_tool_step for workflow_tool_step in workflow.tool["steps"] if shortname(workflow_tool_step["id"]) == shortname(step.tool["id"])][0]
            print "TARGET_STEP: ", target_step
            new_step = {
                "id": shortname(target_step["id"]),
                "run": target_step["run"],
                "in": [],
                "out": [shortname(step_out).split("/")[-1] for step_out in target_step["out"]]
            }
            if target_step.get("scatter", None): new_step["scatter"] = [shortname(scatter_input).split("/")[-1] for
                                                                       scatter_input in
                                                                        target_step["scatter"]] if isinstance(
                target_step["scatter"], list) else shortname(target_step["scatter"]).split("/")[-1]
            if target_step.get("requirements", None): new_step["requirements"] = target_step["requirements"]
            if target_step.get("hints", None): new_step["hints"] = target_step["hints"]
            if target_step.get("label", None): new_step["label"] = target_step["label"]
            if target_step.get("doc", None): new_step["doc"] = target_step["doc"]
            if target_step.get("scatterMethod", None): new_step["scatterMethod"] = target_step["scatterMethod"]

            for step_input in target_step["in"]:
                custom_input = {"id": shortname(step_input["id"]).split("/")[-1]}
                if step_input.get("source", None): custom_input["source"] = [shortname(input_source) for input_source in
                                                                             step_input["source"]] if isinstance(
                    step_input["source"], list) else shortname(step_input["source"])
                if step_input.get("linkMerge", None): custom_input["linkMerge"] = step_input["linkMerge"]
                if step_input.get("default", None): custom_input["default"] = step_input["default"]
                if step_input.get("valueFrom", None): custom_input["valueFrom"] = step_input["valueFrom"]
                new_step["in"].append(custom_input)

            return new_step


        def step_to_workflow (workflow, step):
            new_workflow = {
                "cwlVersion": workflow.tool.get("cwlVersion", "v1.0"),
                "class": "Workflow",
                "inputs": get_inputs(step),
                "outputs": get_outputs(step),
                "steps": []
            }

            custom_step = get_custom_step(workflow, step)
            remove_not_provided_inputs_from_step(custom_step, jobobj) # because of Null bug
            new_workflow["steps"].append(custom_step)

            if workflow.tool.get("$namespaces", None): new_workflow["$namespaces"] = workflow.tool["$namespaces"]
            if workflow.tool.get("$schemas", None): new_workflow["$schemas"] = workflow.tool["$schemas"]
            if workflow.tool.get("requirements", None): new_workflow["requirements"] = workflow.tool["requirements"]

            dirname = os.path.dirname(kwargs["cwl_workflow"])
            filename, ext = os.path.splitext(os.path.basename(kwargs["cwl_workflow"]))
            new_workflow_name = os.path.join(dirname, filename + '_workflow_' + shortname(step.tool["id"]) + ext)
            with open(new_workflow_name, 'w') as generated_workflow_stream:
                generated_workflow_stream.write(json.dumps(new_workflow, indent=4))
            return new_workflow_name


        custom_workflow_file = step_to_workflow (self.dag.cwlwf, self.cwl_step)
        custom_wokflow = cwltool.load_tool.load_tool(argsworkflow = custom_workflow_file,
                                                    makeTool = defaultMakeTool,
                                                    resolver = tool_resolver,
                                                    strict = kwargs['strict'])

        output, status = cwltool.main.single_job_executor(custom_wokflow,
                                                          jobobj,
                                                          makeTool = defaultMakeTool,
                                                          select_resources = None,
                                                          make_fs_access = cwltool.stdfsaccess.StdFsAccess,
                                                          **kwargs)
        if not output and status == "permanentFail":
            raise ValueError

        logging.info(
            '{0}: Embedded tool outputs: \n {1}'.format(self.task_id, json.dumps(output,indent=4)))

        promises = {}
        for out in self.cwl_step.tool["outputs"]:
            out_id = shortname(out["id"])
            jobout_id = out_id.split("/")[-1]
            try:
                promises[out_id] = output[jobout_id]
            except:
                continue

        data = {}
        data["promises"] = promises
        data["outdir"] = self.outdir

        logging.info(
            '{0}: Output: \n {1}'.format(self.task_id, json.dumps(data,indent=4)))

        return data
