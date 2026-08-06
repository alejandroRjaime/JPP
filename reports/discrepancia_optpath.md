# Discrepancia en la tabla de optimization path: 68,10 s vs 89,24 s

Fecha de la medición: 2026-08-05. Máquina: la del repositorio (ver "Condiciones" al final).

## Resumen

**68,10 no es una configuración distinta: es una cifra obsoleta.** Procede de una
generación de medición anterior, con un dataset distinto del que hay hoy en disco.
El manuscrito (89,24 / 14,51) es la fuente internamente consistente. El README del
repositorio arrastra íntegra la tabla antigua.

La medición de hoy no reproduce ninguna de las dos en valor absoluto, pero sí
reproduce **el ratio del manuscrito**, no el del README.

## 1. Qué dice cada fuente

| Fuente | Fila base | Fila intermedia | Fila optimizada | Ratio | Grupos Q1 |
|---|---:|---:|---:|---:|---:|
| `joinless_v8_Jul.tex` (tab:optpath) | 89,24 | — | 14,51 | 6,15× | 200 |
| `joinless_v8_Jul_30.tex` | 89,24 | — | 14,51 | 6,15× | 200 |
| `joinless_v9_Jul_31.tex` | 89,24 | — | 14,51 | 6,15× | 200 |
| `joinless_v10_Jul_31.tex` | 89,24 | — | 14,51 | 6,15× | 200 |
| `README.md:151` | 68,10 | 20,80 | 9,41 | 7,24× | 175 |
| `baselines/README.md:11` | 68,10 (referencia cruzada) | — | — | — | — |
| `baselines/JoinlessBase.scala:10` | 68,10 (comentario) | — | 9,41 | — | — |
| **Medido hoy** | **123,64** | — | **20,27** | **6,10×** | **200** |

Comprobaciones de procedencia:

- **68,10 no aparece en ninguna versión del manuscrito.** Verificado sobre
  `joinless_v8_Jul.tex`, `joinless_v8_Jul_30.tex`, `joinless_v9_Jul_31.tex` y
  `joinless_v10_Jul_31.tex`.
- **89,24 no aparece en ningún punto del repositorio ni de su historial de git.**
  `git log --all -S'89.24'` no devuelve nada.
- 68,10 entra en el repositorio en los commits `bba2b06` y `ba06099`, ambos
  titulados "Add reference implementation and benchmark code".
- El manuscrito es estable en 89,24 / 14,51 a lo largo de las cuatro versiones, y
  14,51 es coherente con **todas** sus demás tablas: resultados principales
  (línea 1638), validación del modelo de coste (1826-1831), tabla de
  particionado (1397) y comparación entre motores (1727).

## 2. De qué configuración sale cada cifra

No son dos configuraciones de la misma variante. Son **dos generaciones de
dataset distintas**, y hay evidencia estructural, no sólo numérica:

1. **Número de grupos.** El README declara 175 grupos; el protocolo vigente y los
   datos en disco dan 200. La dimensión `date` tiene hoy 8 años distintos
   (1992–1999) y `customer` 25 naciones: 8 × 25 = 200. Con 7 años serían 175.
   Una tabla de 175 grupos se midió sobre una dimensión de fecha distinta.

2. **Forma de la tabla.** El README tiene tres filas (68,10 / 20,80 / 9,41); el
   manuscrito tiene dos (89,24 / 14,51). La fila intermedia del README
   ("Indexed array, String values") no existe en el paper. No es una fila que se
   haya recalculado: es una tabla de otra iteración del trabajo.

3. **Ratios.** README 7,24×; manuscrito 6,15×; medido hoy 6,10×. El ratio de hoy
   coincide con el del manuscrito dentro del ruido de una máquina compartida, y
   difiere claramente del README.

## 3. Qué sale hoy

Protocolo aplicado: 5 corridas por configuración, se descarta la primera como
calentamiento, mediana de las 4 restantes. Variantes entrelazadas, no en bloques.

```
=== functional equivalence ===
OK  q1: 200 groups, sum 3,000,078,095,504.6

=== medians (first run of each configuration discarded) ===
query  strategy                    n    median       min       max
q1     base (broadcast HashMap)    4    123.64    116.64    126.38
q1     joinless                    4     20.27     19.91     21.38

=== ratio vs joinless ===
q1: joinless 20.27s   base (broadcast HashMap): 6.10x
```

Ambas variantes coinciden en 200 grupos y en la misma suma agregada, que es la
condición para que la tabla signifique algo.

Regenerable con:

```bash
./scripts/run_optpath.sh ~/ssb_synth 2>&1 | tee ~/optpath.log
python3 scripts/summarize.py ~/optpath.log
```

### Sobre los valores absolutos

Los tiempos de hoy son ~1,39× los del manuscrito en **ambas** filas:

| | Manuscrito | Hoy | Factor |
|---|---:|---:|---:|
| Base | 89,24 | 123,64 | 1,386× |
| Optimizada | 14,51 | 20,27 | 1,397× |

Que el factor sea prácticamente idéntico en las dos filas es la observación
importante: es lo que se espera del **mismo experimento sobre una máquina
uniformemente más lenta**, no de un experimento distinto. Durante la medición
había carga concurrente ajena al benchmark (OpenSearch, Graylog y Apicurio
activos en el mismo host). No es prueba de que la máquina del paper y ésta sean
la misma, pero es consistente con ello y descarta que 89,24/14,51 provengan de
otra configuración lógica.

**No se ha reproducido el valor absoluto del manuscrito.** Sólo su ratio.

## 4. Qué hay que corregir

No he tocado el manuscrito, según lo pedido. Lo que habría que corregir:

### En el repositorio (pendiente de decisión, no aplicado)

- **`README.md`, sección "Reported results — Q1" completa.** No es sólo la fila
  base: las tres tablas (sistemas, optimization path, almacenamiento
  desagregado) y el recuento de 175 grupos pertenecen a la generación obsoleta.
  Mientras siga ahí, el repositorio contradice al paper en la cifra de cabecera
  (9,41 vs 14,51). **No lo he reescrito** porque hacerlo exige remedir también
  DuckDB, ClickHouse, SMJ, BHJ y las filas de MinIO, y esas cifras no las he
  tomado todavía; sustituirlas por las de hoy sólo en algunas filas produciría
  una tabla mezclada, que es peor que la actual.
- **`README.md:117`**: "175 groups" → 200. Este sí es un error verificable
  contra los datos y aislado del resto.

### Ya aplicado en este trabajo

- `baselines/JoinlessBase.scala`: eliminada la referencia a 68,10 s y 9,41 s del
  comentario de cabecera, sustituida por un puntero a este informe y al script
  que regenera ambas filas.
- `baselines/README.md`: la referencia cruzada "the 68.10 s configuration" queda
  desactualizada por lo mismo (pendiente de aplicar junto con la decisión sobre
  el README principal).

### En el manuscrito (sólo señalado)

- Si se decide publicar las cifras remedidas, la tabla `tab:optpath` y **todas**
  las tablas que contienen 14,51 deben moverse a la vez. 14,51 aparece en cinco
  sitios (líneas 1397, 1638, 1674, 1727, 1826-1831, 1883); cambiar sólo
  `tab:optpath` dejaría el paper internamente inconsistente, que es justamente
  el defecto que tiene hoy el repositorio.
- Si se mantienen 89,24 / 14,51, conviene que el paper indique el estado de la
  máquina durante la medición, porque una repetición en una máquina con carga
  concurrente da ~1,4×.

## 5. Por qué la fila no era regenerable

Dos defectos, ambos corregidos:

1. **`scripts/summarize.py` perdía silenciosamente las corridas de la variante
   base.** El parser abría un registro nuevo sólo al ver una línea `query =`, y
   `JoinlessBase` emitía `variant =` sin `query =`. Sus campos se fusionaban
   sobre el registro anterior — corrompiendo además ese baseline — y sus cinco
   corridas colapsaban en una sola que nunca se emitía. Ahora el registro se
   cierra en `elapsed`, que todos los runners imprimen como última línea, y las
   corridas sin etiquetar se reportan como `UNLABELLED` con aviso en lugar de
   asignarse por conjetura.
2. **`JoinlessBase` no emitía la consulta que calcula.** Ahora imprime
   `query = q1`, de modo que entra en el mismo grupo que el resto de
   configuraciones de Q1 y la comprobación de equivalencia funcional lo incluye.

`JoinlessBase.scala` ya estaba presente en `baselines/`, al contrario de lo que
decía el README antiguo; no hubo que extraerlo de ningún sitio.

## 6. Advertencia adicional: el generador no reproduce el dataset

Fuera del alcance de esta tarea, pero afecta a la reproducibilidad de cualquier
cifra de este repositorio. `datagen/GenerateSSB.scala` **no produce el dataset
que hay en `~/ssb_synth`**, y su propia cabecera ya lo advierte. Dos evidencias
independientes:

- El `lineorder` en disco tiene una columna `lo_orderkey` que el generador no
  emite.
- La fórmula `amount = (id % 10000) + 1` daría una suma de verificación exacta de
  **3.000.300.000.000**. La suma real es **3.000.078.095.504,6**, que además no
  es entera.

Es decir: el dataset sobre el que se han tomado todas las cifras no es
regenerable desde el repositorio. Esto es más grave que la discrepancia de la
tabla, porque afecta a todas las tablas a la vez.

## Condiciones de la medición

| | |
|---|---|
| Fecha | 2026-08-05 |
| Repositorio | `/home/athenas/Downloads/jpp/JPP` @ `ec7a6b1` (+ cambios locales) |
| Datos | `/home/athenas/ssb_synth`, `lineorder` 23,4 GB en 2000 ficheros Parquet, 250 particiones Spark |
| Spark | 3.5.8, `local[16]`, driver 32 GB, AQE desactivado, `autoBroadcastJoinThreshold = -1` |
| JDK | 17.0.19 (fijado en `scripts/jpp`; el defecto del host era JDK 23, no soportado por Spark 3.5) |
| Protocolo | 5 corridas, se descarta la primera, mediana de 4; variantes entrelazadas |
| Carga ajena | Sí — OpenSearch, Graylog y Apicurio activos durante la medición |
