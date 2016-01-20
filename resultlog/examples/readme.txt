===============SimRepo-100.n3===========

This file is a service repository consists of 9989 primitive and composite sensor services.

Services are identified by the ?serviceId (a.k.a., service id), which matches the pattern “?serviceId rdf:type ?y graph <http://www.insight-centre.org/ces#> { ?y rdfs:subClassOf ces:EventService}”

There are two types of event services: ces:PrimitiveEventService or ces:ComplexEventService. The difference is that primitive services do not contain event patterns while complex ones do.

In this repository there are 100 complex event services. Others are primitive event services modelled for traffic, air pollution and weather sensors. All primitive events are annotated with functional and non-functional properties.

Both complex and primitive event services  have functionally equivalent services. For primitive ones the those functionally equivalent ones can be easily identified by the their service id: if a primitive event service has an id like “xxx-sim1” it means it is a simulated duplication for sensor “xxx”. For functionally equivalent complex event services, we need to compare the semantics of their event patterns, this has already been done and this information is pertained in “identical_event_patterns.txt”

===============identical_event_patterns.txt.n3============

This file stores a list of lists of identical event patterns, as described above. All event patterns in the sub-lists are functionally equivalent.

===============EventRequest.n3============

This file is an event request used in the composition, annotated in n3. Basically it asks the vehicle counts over 9 different sensors. These sensors have some temporal ids with requested observed property types and feature of interests. Through discovery we can find out the list of sensor ids asked, listed as follows:
"http://www.insight-centre.org/dataset/SampleEventService#230";
"http://www.insight-centre.org/dataset/SampleEventService#228";
"http://www.insight-centre.org/dataset/SampleEventService#226";
"http://www.insight-centre.org/dataset/SampleEventService#220";
"http://www.insight-centre.org/dataset/SampleEventService#218";
"http://www.insight-centre.org/dataset/SampleEventService#216";
"http://www.insight-centre.org/dataset/SampleEventService#540";
"http://www.insight-centre.org/dataset/SampleEventService#537";
"http://www.insight-centre.org/dataset/SampleEventService#322";

The composition task is to find combinations of primitive and/or complex event services in the repository that fulfil the event request, while satisfying the NFP constraints and optimised according to the preferences.

==================ACPs.n3==================

This file is a list of all abstract composition plans (ACP) we created. They are abstract in the sense that all the leaf nodes in the pattern tree can be replaced by other functional equivalent services to create concrete composition plans (CCP). There are only 336 ACPs but there are ~10^10 CCPs.

==================CompositionPlan-1.n3=========================

Composition plan derived by genetic algorithm under constraint: correctness>80%, network consumption < 200 msg/s, preference: correctness_weight = 1.0 network_consumption_weight = 0.1

==================CompositionPlan-2.n3=========================

Composition plan derived by genetic algorithm under constraint: correctness>80%, network consumption < 200 msg/s, preference: correctness_weight = 0.1 network_consumption_weight = 1.0
