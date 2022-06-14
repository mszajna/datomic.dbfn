(ns datomic.dbfn
  (:require [clojure.walk :as w]
            [backtick :as b]
            [riddley.walk :as r]
            [riddley.compiler :as rc]
            [datomic.api :as d]))

(def mfunction
  "Memoized d/function, installed on transactor automatically by call.
   Use invalidate-fn-cache to invalidate."
  (memoize d/function))

(def ^:private mf `mfunction)

(defn force-fn
  "Force db-fn created with datomic.api/function to compile immediately."
  [db-fn]
  (force (:fnref db-fn))
  db-fn)

(defn quote-unquotes [form]
  (let [quot (fn [x] `'~x)]
    (w/postwalk
    #(cond
        (b/unquote? %) (quot %)
        (b/unquote-splicing? %) (list* (first %) (map quot (rest %)))
        :else %)
    form)))

(defn ser
  "Serializes the db-fn for use in another txn."
  [db-fn]
  (cond-> (dissoc db-fn :fnref)
    (empty? (:requires db-fn)) (dissoc :requires)
    (empty? (:imports db-fn)) (dissoc :imports)))

(defn quoted-dbfn? [form]
  (and (seq? form)
       (= (first form) 'quote)
       (instance? datomic.function.Function (second form))
       (= "clojure" (name (:lang (second form))))))

; TODO: try serializing dbfns as inline functions instead of d/functions.
;       - need to unify requires. Perhaps could only support :as requires?
;       - at which point perhaps could infer reqs, but what about imports?
(defn ser-dbfns' [form]
  (let [c (gensym "c")
        ser-dbfn-form #(if (quoted-dbfn? %) `(~c (quote ~(ser (second %)))) %)
        updated (w/postwalk ser-dbfn-form form)]
    (if (not= updated form)
     `(let* [~c (resolve '~mf) ~c (if ~c @~c ~`d/function)] ~updated)
      form)))

; This does the trick, bar handling requires/imports.
(defn ser-dbfns [form]
  (let [inline (fn [f] `(~'fn ~(:params f) ~(read-string (:code f))))]
    (w/postwalk
      #(if (quoted-dbfn? %) (inline (second %)) %)
      form)))

; Requires can be unified assuming they're aliased.
; Conflicting imports can't be resolved without qualifying
; all symbols, which requires macroexpanding-all.
; An option is to allow inlining only when imports and requires don't conflict.
(defn unify-reqs [reqs])

; Alternatively I could macroexpand and provide a d/transaction middleware to
; attempt not to send the entire function, just its hash or smth.
; Would have to use a strong hashing function
(defn with-cache [tr]
  (fn [conn tx]
    (letfn [use-fn-hash-if-exists
            ([st]
              (if (and (= (first st) ::invoke) (:hash (second st)))
                (dissoc (update st 1 :hash) :fnref)
                st))]
      (try (tr conn (map use-fn-hash-if-exists tx))
        (catch Throwable t ; TODO: narrow down the catch
          (tr conn tx))))))

; walk the form and return all global vars & classes referenced
(defn walk [form env]
  (cond
    (symbol? form) (when-let [r (resolve env form)] [r])
    (seq? form)
    (let [head (first form)
          resolved (resolve env head)]
      (cond
        (and resolved (.isMacro resolved))
        (concat [resolved] (walk (apply resolved form env (rest form)) env))
        (= 'fn* head)
        (for [[binds & body] (drop-while #(not (seq? %)) form)
              :let [env (merge env (zipmap binds (repeat true)))]]
              res (walk body env)]
          res)
        (= 'let* head)
        (let [[[bind exp & binds] & body] (rest form)]
          (if bind
            (concat (walk exp env)
                    (walk `(let* [~binds] ~@body) (assoc env bind true)))
            (mapcat #(walk % env) body)))
        (= 'loop* head) (walk `(let* ~@(rest form)) env)
        (= 'letfn* head)
        (let [binds (partition 2 (second form))
              env (merge env (zipmap (map first binds) (repeat true)))]
          (concat (mapcat #(walk (second %) env) binds)
                  (mapcat #(walk % env) (drop 2 form))))
        ; TODO: try/catch/finally
        :else (mapcat #(walk % env) form)))
    (or (vector? form) (map? form) (set? form)) (mapcat #(walk % env) form)))


(defmacro txn*
  "A variant of txn that blurs the line of local/transactor divide even further
   with body macro-expanded on client, requires and imports inferred from
   *ns* and symbols from namespaces unavailable on transactor automatically (?)
   inlined, including inlining of db-fns. txn*s cannot refer regular non-db fns."
  [params & body]
  ; TODO: use riddley to macroexpand, resolve symbols, inline db-fns as regular
  ; functions unifying requires and imports, inline locals and vars for ns that
  ; are not whitelisted.
  ; Consideration: many macros expand to code referencing non-core symbols.
  ; Auto-inline doesn't help much as these are mostly fn-calls, resulting
  ; in confusing errors about not being able to embed objects in code. I could
  ; detect these problems early for warnings to name the offending symbol but
  ; tracing it back to source code is still tricky as the symbol comes from
  ; macro-expansion.

  ; Reg. riddley - it seems riddley eagerly macroexpands - there's no way to capture
  ; macro symbols for 'requires' with it
  nil)

; IDEA: install db functions under their hash with a middleware?
; Embedding functions is technically possible, if inconvenient. It feels like
; a lot of work and trade-offs for a solution that is a leaky abstraction
; (vars resolved to values eagerly - can't redef; function bodies grow
; uncontrollably). I could register all defined dbfns at compilation time,
; and install them on startup or smth perhaps?

(defmacro txn
  "Returns a Datomic #db/fn of params that evaluates to body. Note that
   body is not a closure and has to require/import any vars outside
   [clojure.core :refer :all], [datomic.api :as d] and java.lang.* before use
   with the optional requires+imports? map under :requires and :imports.
   Local context can be accessed in compile time using unquote.

   Usage:
   Run #db/fn locally
   => (let [f (txn [db arg] ..)] (f (d/db conn) arg-val))

   Use in transactions once installed
   => (d/transact conn [{:db/ident :my-fn :db/fn (txn [db arg] ..)}])
   => (d/transact conn [[:my-fn arg-val]]) ; db argument is applied implicitly,

   Use in transactions directly with :datomic.dbfn/call (see call in this ns)
   => (d/transact conn [[:datomic.dbfn/call (txn [db arg] ..) arg-val]])

   See also https://docs.datomic.com/on-prem/database-functions.html"
  {:arglists '([requires+imports? [params*] body])}
  [& args]
  (let [[{:keys [requires imports]} params & body]
          (if (map? (first args)) args `(nil ~@args))
        code (if (= 1 (count body)) (first body) `(do ~@body))]
   `(-> {:lang :clojure
         :requires '~requires
         :imports '~imports
         :params '~params
         :code (pr-str (ser-dbfns (b/template ~(quote-unquotes code))))}
        d/function
        force-fn)))

(defmacro deftxn
  "Same as (def name doc-string? (txn [params*] body))."
  {:arglists '([name doc-string? requires+imports? [params*] body])}
  [name & args]
  (let [[doc & args] (if (string? (first args)) args `(nil ~@args))]
   `(def ~name ~@(when doc [doc]) (txn ~@args))))

(defn- ns-sym [s] (symbol (namespace s)))
(defn- name-sym [s] (symbol (name s)))

(deftxn call
  "Compiles the db-fn using datomic.api/function and applies it db and args.
  Caches compilation on the transactor indefinitely, unless invalidated
  with invalidate-fn-cache.

  Usage:
  => (require '[datomic.dbfn :as t])
  => (d/transact conn [t/call-install-tx]) ; run once
  => (t/deftxn my-tx [db arg] ..)
  => (d/transact conn [[::t/call my-tx arg-val]])"
  [db dbfn & args]
  (let [compile-dbfn
        @(or (resolve ~mf)
              (intern (create-ns ~(ns-sym mf)) ~(ns-sym mf)
                      (memoize d/function)))]
    (apply (compile-dbfn dbfn) db args)))

(deftxn invalidate-fn-cache
  "Invalidate the compilation cache of call. Datomic serializes #db/fn to a map
  which is then used as the cache key guaranteeing that the cache never goes
  stale. #db/fns don't close over the lexical scope and so tend not to occupy
  much memory either. You should rarely need this function.

  Usage:
  => (d/transact conn [[:datomic.dbfn/call invalidate-dbfn-cache]])"
  [_]
  (ns-unmap (create-ns ~(ns-sym mf)) ~(name-sym mf)))

(def call-install-tx
  "Transaction statement to install :datomic.dbfn/call, see call in this namespace."
  {:db/ident ::call :db/fn call})
