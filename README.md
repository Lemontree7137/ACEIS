# ACEIS
Middleware for complex event service

The Automatic Complex Event Implementation System (ACEIS) is a middleware for complex event services. It is implemented to fulfill large-scale data analysis requirements in the CityPulse project and is responsible for event service discovery, composition, deployment, execution and adaptation. In the CityPulse project, data streams are annotated as event services, using the Complex Event Service Ontology. Event service discovery and composition refer to finding a data stream or a composite set of data streams to address the user-defined event request. Event service deployment refers to transforming the event service composition results (a.k.a., composition plans) into RDF Stream Processing (RSP) queries and registering these queries to relevant RSP engines, e.g., CQELS and C-SPARQL. Event service execution refers to establishing the input and output connections for the RSP engines to allow them consuming real-time data from lower-level data providers and delivering query results to upper-level data consumers. Event services adaptation ensures the quality constraints over event services are ensured at run-time, by detecting quality changes and make adjustments automatically.

ACEIS can be used in scenarios where on-demand discovery and composition of data streams are needed, and RSP is used for evaluating queries over the data streams e.g.:
1) Scenario 1: monitoring traffic conditions over different streets and regions in a city for a city administrator.
2) Scenario 2: planning the travel routes for a citizen based on his/her functional and non-functional requirements.
ACEIS cannot be used in situations where queries or streams used for the queries are fixed, i.e., no on-demand stream discovery and composition possible/necessary.

ACEIS has the following limitations:
1) Event Semantics: The expressiveness of event requests with regard to temporal and logical correlations is limited to AND, OR, SEQUENCE and REPETITIONS, as described in this paper, and the events modeled in ACEIS are instant events, i.e., only one timestamp allowed for each event. Interval-based events are not supported.
2) RSP Engine Types: Currently only CQELS and C-SPARQL engines are supported for event service execution. However, a third-party developer can integrate new engines by extending the query transformation module.
3) Concurrent Queries: Existing RSP engines are still in their early stages and there is room for performance optimization. Currently, the data federation component can handle approximately 1000 CQELS or 90 C-SPARQL queries in parallel, by applying a load-balancing technique. A larger number of concurrent queries may (depending on the query complexity) result in unstable engine status.

ACEIS can be extended to:
1) support more expressive event semantics by extending the event models, event reusability definitions and the event service composition algorithms,
2) support more RSP engines by extending the query transformation algorithms and
employ more advanced adaptation algorithms.

More instructions on how to use ACEIS can be found at http://fenggao86.github.io/ACEIS/
