# Missing links predictions

#### Roadmap

- [x] Сгенерировать граф
- [x] Сохранить граф, как список ребер
- [x] Найти второй круг
- [ ] Посчитать метрики для второго круга
- [ ] Сохранить метрики
- [ ] Сжать списки ребер с CSR + delta encoding. Положить индекс в каждый чанк
- [ ] Отрефакторить код, чтобы можно было считать линейные комбинации

#### Graph Generation

Наивная реализация алгоритма BA работает за O(n^2), где n - кол-во вершин в генерируемом графе.

В статье [Roulette-wheel selection via stochastic acceptance](https://scholar.google.fr/scholar?cluster=3862086056988553103) предлагают алгоритм, который работает за O(n ^ 1.5).

#### Missing links metrics

Типичные метрики для поиска недостающих ребер: [Predicting missing links via local information](https://scholar.google.fr/scholar?cluster=12704085315179052707)

#### Serialization format

Граф большой, если хранить в лоб выходит ~ 80 гб (10000000 ребер x 8 байт на ребро). Можно хранить как постинг листы в индексе. 
Пример и анализ есть в статье [Working with Graphs in Main Memory, Compact Storage](http://g-store.sourceforge.net/th/3.htm). 
И delta-encoding добавим, чтобы еще немножко выжать, длины чисел только уменьшатся.

Изначально реализуем алгоритмы на простой сериализации, потом добавим MR джобы, которые переводят списки ребер в сжатый вид и обратно.

#### Second Circle on MR

Делаем классический BFS на Pregel, ищем вершины на расстоянии 2. В итоге получим в каждой вершине потенциальных друзей.

Подробности в треде на [Stackoverflow, BFS on MR](https://stackoverflow.com/questions/20774253/hadoop-map-reduce-for-google-web-graph)

#### Метрики расстояния

Считаем метрики и кладем их в каждую вершину. Делаем Reduce по вершинам, считаем линейную комбинацию.
