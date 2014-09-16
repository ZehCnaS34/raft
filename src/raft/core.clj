(ns raft.core
  (:require [clojure.tools.logging :as l]
            [clojure.core.async :refer [put! map< alts!! <!!] :as async]
            [midje.open-protocols :refer [defrecord-openly]]))


(defprotocol IPersist
  (persist [raft] "Persists the essential state of the raft"))


(defrecord Entry [term command maybe-execution-chan])

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


(defn- call-rpc
  [{rpc :rpc
    current-term :current-term
    this-server :this-server
    :as raft} command [server params]]
  (map< #(vector server %)
    (apply rpc
           server command current-term this-server
           (last-index raft)
           (last-term raft)
           params)))


(defn- call-rpcs [raft command server-params]
  (->> server-params
       ; make a seq of channels with responses
       (map (partial call-rpc raft command))
       ; make a channel with all responses
       async/merge
       ; make a channel with a map of server to their response
       (async/reduce conj {})))


(defn send-rpc
  "Send a remote procedure call to the selected servers."
  ([raft command server-params] (send-rpc raft command server-params nil)) 
  ([raft command server-params timeout]
    (l/debug
      "Sending RPC" command
      "with parameters" server-params
      "and timeout" timeout)
    ; TODO add some handling/logging for things timing out
    (if timeout
      (or 
        (first 
          (alts!! [(call-rpcs raft command server-params)
                   (async/timeout timeout)]))
        {})
      (<!! (call-rpcs raft command server-params)))))


(defn send-rpc-to-all
  "Send a remote procedure call with the same parameters to all servers."
  ([raft command params] (send-rpc-to-all raft command params nil)) 
  ([raft command params timeout]
    (send-rpc raft command
      (->> raft
          :servers
          keys
          (map (juxt identity (fn [_] params))))
      timeout)))


(defn- exec-state-machine
  [{:keys [state-machine] :as raft} {:keys [command maybe-execution-chan]}]
  {:pre [(not (nil? state-machine))]}
  (let [[ value new-state-machine] (state-machine command)]
    (when maybe-execution-chan
      (put! maybe-execution-chan {:value value}))
    (assoc raft :state-machine new-state-machine)))


(defn apply-commits
  [raft new-commit-index]
  (l/debug "apply-commits" new-commit-index)
  (let [commit-index (or (:commit-index raft) -1)
        raft (assoc raft :commit-index new-commit-index)]
    (assert (> (count (:log raft)) commit-index))
    (assert
      (or (nil? new-commit-index) (> (count (:log raft)) new-commit-index)))
    (assert (or (nil? new-commit-index) (>= new-commit-index commit-index)))
    (if-not (nil? new-commit-index)
      (reduce exec-state-machine raft
              (subvec (:log raft) (inc commit-index) (inc new-commit-index)))
      raft)))


;
; create-raft
;
; `rpc` should be a function (fn [server rpc-name & args] (thread result))
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
