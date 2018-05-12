(ns examples.imdb)

(def schema
  {:genres {:multivalued? true}
   :directors {:multivalued? true
               :reference? true}
   :knownForTitles {:multivalued? true
                    :reference? true}
   :primaryProfession {:multivalued? true}})
