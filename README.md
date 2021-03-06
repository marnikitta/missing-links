# Missing links predictions

#### Roadmap

- [x] Сгенерировать граф
- [x] Сохранить граф, как список ребер
- [x] Найти второй круг
- [x] Посчитать метрики для второго круга
- [x] Сохранить метрики
- [x] Сжать списки ребер delta encoding
- [x] Отрефакторить код, чтобы можно было считать линейные комбинации

#### Related papers

- [Map-Reduce based Link Prediction for Large Scale Social Network](https://ksiresearchorg.ipage.com/seke/seke17paper/seke17paper_100.pdf)

#### Генерация графа

Наивная реализация алгоритма BA работает за O(n^2), где n - кол-во вершин в генерируемом графе.

В статье [Roulette-wheel selection via stochastic acceptance](https://scholar.google.fr/scholar?cluster=3862086056988553103) предлагают алгоритм, который работает за O(n ^ 1.5).

#### Сериализация

Граф большой, если хранить в лоб выходит ~ 80 гб (10000000 ребер x 8 байт на ребро). Можно хранить как постинг листы в индексе. 
И delta-encoding добавим, чтобы еще немножко выжать, длины чисел только уменьшатся.

_Каноническое предствление_ - оставляем только ребра ведущие из вершины с меньшим номером в вершину с большим. 
Списки смежных ребер сортируем. При этом кол-во ребер уменьшится вдвое, а сортированные списки помогут сделать delta-encoding эффективнее.

Функции `GraphCanonizer` и `GraphDecanonizer` переводят между двумя представлениями.

Функции `GraphEncoder` и `GraphDecoder` сжимают канонический граф и разжимают соответственно.

#### Метрики

Типичные метрики для поиска недостающих ребер: [Predicting missing links via local information](https://scholar.google.fr/scholar?cluster=12704085315179052707).

Большую часть из них можно представить в виде `dist(x, y) = sum_common(f(x, common, y))`, где `common` лежит в множестве смежных вершин как с `x` так и с `y`.
Например для _Common Neighbours_ `f(x, common, y) = 1`, а для _Adamic-Adar_ - `f(x, common, y) = 1 / log(degree(common))`

Для подсчета линейной комбинации метрик в качестве `f` надо взять линейную комбинацию `f_i`.

#### Поиск недостающих ребер

Для поиска ребер и метрик для них взят MR алгоритм из статьи [A New Link Prediction Algorithm Based on Local Links](https://scholar.google.fr/scholar?cluster=10529432855783499444)

1. Для каждой вершины `z` породить тройки вершин, в которых `z` является промежуточной

`z: [to0, to1, to2...] => (to0, z, to1), (to0, z, to2), (to1, z, to2) ...`

2. Для каждой тройки посчитать `f`

3. Для каждой пары вершин `to0, to1` просуммировать частичные значения метрик. В результате получится полная метрика `dist(to0, to1)`.

4. Для каждой вершины взять 500 ближайших вершин



