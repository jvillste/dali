(ns argumentica.transducible-collection)

(defprotocol TransducibleCollection
  (transduce [this from-key options]))


;; (extend-protocol TransducableCollection
;;     clojure.lang.Sorted
;;   ;; clojure.lang.PersistentTreeSet
;;   (transduce2 [this value options]
;;     (let [transducer (or (:transducer options)
;;                          identity)
;;           reducing-function (transducer (or (:reducer options)
;;                                             (constantly nil)))
;;           direction (or (:direction options)
;;                         :forwards)]
;;       (prn (:reducer options)
;;            (if (= :forwards direction)
;;              (subseq this >= value)
;;              (rsubseq this <= value))) ;; TODO: remove-me

;;       (clojure.core/transduce transducer
;;                               reducing-function
;;                               (if (= :forwards direction)
;;                                 (subseq this >= value)
;;                                 (rsubseq this <= value))))))

;; (comment

;;   (clojure.core/transduce identity
;;                           conj
;;                           (subseq (sorted-set 1 2 3) >= 2))

;;   (clojure.core/transduce identity
;;                           conj
;;                           (subseq (sorted-set 1 2 3) >= 2))

;;   (transduce2 (sorted-set 1 2 3)
;;               1
;;               {:reducer conj}
;;              )
;;   ) ;; TODO: remove-me


