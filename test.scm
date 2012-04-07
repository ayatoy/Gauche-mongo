(use gauche.test)

(test-start "mongo")

(use mongo.util)
(test-module 'mongo.util)

(use mongo.bson)
(test-module 'mongo.bson)

(use mongo.wire)
(test-module 'mongo.wire)

(use mongo.node)
(test-module 'mongo.node)

(use mongo.core)
(test-module 'mongo.core)

(use mongo)
(test-module 'mongo)

(test-end)
