(ns metered.usage
  (:require [clj-time.core :as time]
            [clj-time.coerce :as time.coerce]))

(defn billable?
  [events]
  (and (= 1 (->> events (map :usage/account) set count))
       (= 1 (->> events (map :usage/uuid) set count))
       (= [:usage.event/create :usage.event/destroy]
          (map :usage/event events))
       (let [[t0 t1] (map :usage/timestamp events)]
         (or (time/before? t0 t1) (time/equal? t0 t1)))))

(defn create-statement
  [from-event to-event]
  {:pre [(map? from-event)
         (map? to-event)]}
  (let [t0 (:usage/timestamp from-event)
        t1 (:usage/timestamp to-event)
        d  (time/in-minutes (time/interval t0 t1))]
   (-> from-event
       (select-keys [:usage/resource
                     :usage/uuid
                     :usage/account])
       (assoc :usage/duration d))))

(defn process-usage
  [events]
  (->> events
       (map #(update % :usage/timestamp time.coerce/from-date))
       (group-by (juxt :usage/account :usage/uuid))
       vals
       (filter billable?)
       (map (partial apply create-statement))))
