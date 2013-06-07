(ns raft.heartbeat-test
  (:use midje.sweet)
  (:require [raft.heartbeat :refer :all]
            [raft.core :refer :all]
            [raft.leader :as leader]))


(facts "about heartbeat"
       (fact "resets election timeout if it hasn't been set"
             (let [raft (create-raft --rpc-- ..store.. ..state-machine.. ..server.. ..servers.. :election-timeout 10)]
               (heartbeat raft)) => (contains {:election-timeout-remaining #(>= % 10)})
             (let [raft (-> (create-raft --rpc-- ..store.. ..state-machine.. ..server.. ..servers.. :election-timeout 10)
                          (assoc :election-timeout-remaining 1))]
               (heartbeat raft)) => (contains {:election-timeout-remaining 1}))
       (fact "becomes a candidate if not a leader and election timeout expires"
             (let [raft (-> (create-raft --rpc-- ..store.. ..state-machine.. ..server2.. [..server2.. ..server3..] :election-timeout 10)
                          (assoc :election-timeout-remaining 0))]
               (heartbeat raft) => anything
               (provided
                (leader/become-candidate raft) => anything))
             (let [raft (create-raft --rpc-- ..store.. ..state-machine.. ..server2.. [..server2.. ..server3..] :election-timeout 10)]
               (heartbeat raft) => anything
               (provided
                (leader/become-candidate raft) => anything :times 0)))

       (future-fact "pushes append-entries RPC to followers if leader"))
