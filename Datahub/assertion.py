import logging
import random
import time

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    CustomAssertionInfoClass,
    AssertionRunEventClass,
    AssertionTypeClass,
    AssertionRunStatusClass,
    AssertionResultClass,
    AssertionResultTypeClass,
)
from datahub.emitter.mce_builder import make_assertion_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

log = logging.getLogger(__name__)

endpoint = "http://localhost:8080"
token = ""

graph = DataHubGraph(
    config=DatahubClientConfig(
        server=endpoint,
        token=token
    )
)

emitter = DatahubRestEmitter(
    gms_server=endpoint,
    token=token
)

dataset_urn = "urn:li:dataset:XXX"

assertion_urn = make_assertion_urn("jonnyTestAssertion")

assertion_info = AssertionInfoClass(
    type=AssertionTypeClass.DATASET,
    description="Check if x equals x",
    customAssertion=CustomAssertionInfoClass(
        type="CUSTOM",
        entity=dataset_urn,
        logic="x = x",
    )
)

mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_info
)

emitter.emit(mcp)

#existing_assertion_urn = "urn:li:assertion:expect_table_columns_to_match_ordered_list"

res1 = graph.report_assertion_result(
    #urn=existing_assertion_urn,
    urn=assertion_urn,
    timestamp_millis=int(time.time() * 1000),
    type="SUCCESS",
    properties=[{"key": "custom_field_1", "value": str(random.random())},
                {"key": "custom_field_2", "value": str(random.random())}],
)

time.sleep(5)

res2 = graph.report_assertion_result(
    #urn=existing_assertion_urn,
    urn=assertion_urn,
    timestamp_millis=int(time.time() * 1000),
    type="FAILURE",
    properties=[{"key": "custom_field_1", "value": str(random.random())},
                {"key": "custom_field_2", "value": str(random.random())}],
    error_type="UNKNOWN_ERROR",
    error_message="The assertion failed due to an unknown error"
)

if res1 and res2:
    log.info("Successfully reported Assertion Result!")

assertion_run_event = AssertionRunEventClass(
    timestampMillis=int(time.time() * 1000),
    assertionUrn=assertion_urn,
    runId="run-123",
    result=AssertionResultClass(
        type=AssertionResultTypeClass.SUCCESS,
        actualAggValue=90,
        externalUrl="http://example.com/uuid1",
    ),
    status=AssertionRunStatusClass.COMPLETE,
    asserteeUrn=dataset_urn,
)

mcp = MetadataChangeProposalWrapper(
    entityUrn=assertion_urn,
    aspect=assertion_run_event
)

emitter.emit(mcp)
