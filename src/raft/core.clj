(ns raft.core)


(defprotocol IPersist
  (persist [raft] "Persists the essential state of the raft"))


(defrecord Raft [rpc store log current-term this-server servers
                 election-timeout state-machine leader-state voted-for
                 election-timeout-remaining commit-index]
  IPersist
  (persist [raft]
    (let [store (:store raft)]
      (store :log (:log raft))
      (store :current-term (:current-term raft))
      (store :voted-for (:voted-for raft)))
    raft))


(def last-index (comp #(when-not (neg? %) %) dec count :log))

(def last-term (comp :term last :log))


;
; create-raft
;
; `rpc` should be a function (fn [server rpc-name & args] (future result))
; where server is an opaque entry from the servers sequence.
;
; `store` should be a function (fn ([key value] nil) ([key] value))
; that persists the keys and their associated values to a non-volatile
; media. The 1-arity function should return nil if the key is not stored.
; The persistence mechanism should be synchronous.
;
; `state-machine` should be a function (fn [input] [result new-state-machine])
; where input is the command to be executed by the state machine from the log,
; result is the output generated by executing the command, and
; new-state-machine is a function like the original state machine function
; but closed over the new state.
;
; `this-server` should be the server identifier for this server.
;
; `servers` should be a sequence of server identifiers, excluding this
; server.
;
; `election-timeout` is the minimum amount of time (in milliseconds) the server
; will wait before trying to become leader itself during an election.
;
; `election-term` is the election term that the server will start on. It
; defaults to 0.
;
;
(defn create-raft
  [rpc store state-machine this-server servers & {:keys [election-timeout election-term]
                                                  :or {election-timeout 150 election-term 0}}]
  (Raft. rpc store
         (or (store :log) [])
         (or (store :current-term) election-term)
         this-server
         (into {} (mapv #(vector % {}) servers))
         election-timeout
         state-machine
         :follower
         (store :voted-for)
         nil
         nil))
