(ns raft.core
  (:use clojure.tools.logging)
  (:use midje.open-protocols))


(defprotocol IPersist
  (persist [raft] "Persists the essential state of the raft"))


(defrecord-openly Raft [rpc store log current-term this-server servers
                        election-timeout state-machine leader-state voted-for
                        election-timeout-remaining commit-index]
  IPersist
  (persist [raft]
    (let [store (:store raft)]
      (store :log (:log raft))
      (store :current-term (:current-term raft))
      (store :voted-for (:voted-for raft)))
    raft))


; Returns nil if log is empty.
(def last-index (comp #(when-not (neg? %) %) dec count :log))

(def last-term (comp :term last :log))


; An internal wrapper around the RPC function we're given.
(defn- call-rpc
  "Calls the external RPC function and blocks on receiving a response."
  [raft server command params]
  [server @(apply (:rpc raft)
                  server
                  command
                  (:current-term raft)
                  (:this-server raft)
                  (last-index raft)
                  (last-term raft)
                  params)])


; Call the RPC on all servers, passing the provided parameters.
; `params` should be either a list of parameters that are sent
; to all servers, or should be a map from server identifier to
; the list of parameters to send to each server.
; If `params` is a map and a server is ommitted from it, the
; server will not be sent an RPC.
; If the timeout is provided, the operation will abort if it
; takes longer than `timeout` milliseconds.
; TODO just split this into two functions
(defn send-rpc
  "Send a remote procedure call to all servers."
  ([raft command params timeout]
   (debug "Sending RPC" command "with parameters" params "and timeout" timeout)
   (let [get-params (fn [server]
                      (debug "getting server params for server" server "from params" params)
                      (if (map? params)
                        (get params server nil)
                        params))
         rpc (fn [[server params]]
               (call-rpc raft server command params))
         requests (->> raft
                    :servers
                    keys
                    (map (juxt identity get-params))
                    (remove #(nil? (second %))))
         requests-agents (map agent requests)]
     (doseq [request requests-agents]
       (set-error-mode! request :continue)
       (set-error-handler! request (fn [ag ex]
                                     (error "rpc failure" ag ex)))
       (send-off request rpc))
     (if timeout
       (apply await-for timeout requests-agents)
       (apply await requests-agents))

     (debug "Assembling responses for RPC")
     ; Deref's on agents don't block.
     (map (fn [r] (when-not (agent-error r) @r)) requests-agents)))
  ([raft command params]
   (send-rpc raft command params nil)))


(defn apply-commits
  [raft new-commit-index]
  (debug "apply-commits" new-commit-index)
  (let [commit-index (or (:commit-index raft) -1)
        raft (assoc raft :commit-index new-commit-index)]
    (assert (> (count (:log raft)) commit-index))
    (assert
      (or (nil? new-commit-index) (> (count (:log raft)) new-commit-index)))
    (assert (or (nil? new-commit-index) (>= new-commit-index commit-index)))
    (if-not (nil? new-commit-index)
      (loop [state-machine (:state-machine raft)
             entries (subvec (:log raft)
                             (inc commit-index)
                             (inc new-commit-index))]
        (assert (not (nil? state-machine)))
        (if (seq entries)
          (recur (second (state-machine (:command (first entries))))
                 (rest entries))
          raft))
      raft)))


;
; create-raft
;
; `rpc` should be a function (fn [server rpc-name & args] (future result))
; where server is an opaque entry from the servers sequence (or this-server).
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
  [rpc store state-machine this-server servers &
   {:keys [election-timeout election-term]
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
