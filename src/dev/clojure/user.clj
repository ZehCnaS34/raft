(ns user
  (:refer-clojure :exclude [< >])
  (:import (org.zeromq SocketType ZMQ ZContext)
           (java.io ByteArrayOutputStream ByteArrayInputStream))
  (:require [clojure.core.async :as asy]
            [cognitect.transit :as t]
            [clojure.pprint :as pp]
            [integrant.core :as ig]
            [integrant.repl]))

(def zmq-status #{:boot :write :process :read :error :kill})

(integrant.repl/set-prep!
 (constantly {:raft/p1   {:address "tcp://*:3449" :type :rep}
              :raft/p2   {:address "tcp://*:3449" :type :req}
              :raft/user {:p1 (ig/ref :raft/p1)
                          :p2 (ig/ref :raft/p2)}}))

(defn reset []
  (integrant.repl/reset))

(def socket-map
  {:req SocketType/REQ
   :rep SocketType/REP})

(def op-status*
  {:req :write
   :rep :read})

(defn exit [code message]
  (println message)
  (System/exit 1))

(deftype Tether [^:mutable status
                 options
                 write
                 read
                 ^:mutable context
                 ^:mutable socket])

(defn get-options
  [^Tether t]
  (.-options t))

(defn get-socket
  [^Tether t]
  (.-socket t))

(defn get-write
  [^Tether t]
  (.-write t))

(defn get-status
  [^Tether t]
  (.-status t))

(defn set-status
  [^Tether t s]
  (set! (.-status t) s))

(defn set-context
  [^Tether t s]
  (set! (.-context t) s))

(defn set-socket
  [^Tether t s]
  (set! (.-socket t) s))

(defn make-zmq
  [opts]
  (Tether. :boot opts (asy/chan) (asy/chan) nil nil))

(defmulti tick
  (fn [^Tether system & args]
    (get-status system)))

(defmethod tick :boot
  [^Tether system]
  (let [context        (ZContext.)
        [type address] ((juxt :type :address) (get-options system))
        socket         (.createSocket context (socket-map type))]
    (assert address "Address must be defined.")
    (case type
      :rep (.bind socket address)
      :req (.connect socket address))
    (set-context system context)
    (set-socket system socket)
    (set-status system (op-status* type))
    system))

(defmethod tick :write
  [^Tether system]
  (let [socket (get-socket system)
        write  (get-write system)
        out    (ByteArrayOutputStream.)
        writer (t/writer out :json)]
    (assert socket "Socket must be defined.")
    (assert write "Write channel must be defined.")
    (if-let [value (asy/<!! write)]
      (do
        (t/write writer value)
        (.send socket out 0)
        system
        (assoc system :status :read))
      (assoc system :status :dead))))

(defmethod tick :read
  [^Tether system]
  (let [[socket read] ((juxt :socket :read) system)]
    (assert socket "Socket must be defined.")
    (assert read "Read channel must be defined.")
    (if-let [reader (t/reader (.recv socket 0) :json)]
      (do
        (asy/>!! read (t/read reader))
        (assoc system :status :write))
      (assoc system :status :dead))))

(defmethod ig/init-key :raft/p1
  [_ options]
  (make-zmq options))

(defmethod ig/init-key :raft/p2
  [_ options]
  (make-zmq options))

(defmethod ig/init-key :raft/user
  [_ {p1' :p1 p2' :p2}]
  (def p1 p1')
  (def p2 p2')
  )

(comment
  (get-status p1)
  (tick p1)

  (get-status p2)
  )
