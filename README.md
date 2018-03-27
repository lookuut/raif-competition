<h1>Соревнования от Райффайзен банка в boosters.pro</h1>
<p>Задача: используя координату, адрес, дату, код платежа, страну, город, валюту и id терминала 
предсказать координаты места жительства и координаты работы клиента банка</p>
<p>Цели участия - изучить язык программирования Scala, Apache Spark и алгоритмы машинного обучения</p>
<p>Для решения задачи были применены алгоритмы от mllib DecisionTree, RandomForest, KMeans, 
DBSCAN. Также xgboost binary classification. Помимо алгоритмов машинного обучения, были применены технологии spark Pipeline, 
CrossValidator</p>

<p>Использованные библиотеки и исходники: <br/>
esri geometry https://github.com/Esri/geometry-api-java <br/>
DBSCAN https://github.com/mraad/dbscan-spark <br/>
xgboost https://github.com/dmlc/xgboost <br/>
Spark mllib 
</p>
<p>Краткий итог: нативные либы от Spark показали себя не очень, максимальный скор был при комбинировании DBSCAN, RandomForest и моим самописным CustomerPointFeature - 0.31. XGBoost с CrossValidation показали себя лучше - 0.365266, 69 место финальном рейтинге</p>
