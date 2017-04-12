```
# Divisive-Analysis-Clustering-
<console>:46: error: constructor cannot be instantiated to expected type;
 found   : org.apache.spark.mllib.linalg.distributed.IndexedRow
 required: org.apache.spark.mllib.linalg.Vector
             case IndexedRow(i, value) => value.toArray.sum / (m-1)
                  ^
<console>:51: error: value max is not a member of Array[Nothing]
           val maxAveRowSum = aveSumRowArray.max
                                             ^
<console>:55: error: value indexOf is not a member of Array[Nothing]
           aveSumRowArray.indexOf(maxAveRowSum)
                          ^
<console>:70: error: constructor cannot be instantiated to expected type;
 found   : org.apache.spark.mllib.linalg.distributed.IndexedRow
 required: org.apache.spark.mllib.linalg.Vector
             case IndexedRow(i, value) => value.toArray.sum / (m - 1)
                  ^
<console>:75: error: constructor cannot be instantiated to expected type;
 found   : org.apache.spark.mllib.linalg.distributed.IndexedRow
 required: org.apache.spark.mllib.linalg.Vector
             case IndexedRow(i, value) => value.toArray.sum / q
                  ^
<console>:79: error: type mismatch;
 found   : org.apache.spark.rdd.RDD[Nothing]
 required: org.apache.spark.rdd.RDD[U]
Note: Nothing <: U, but class RDD is invariant in type T.
You may wish to define T as +T instead. (SLS 4.5)
           val diffSum = aveSumRow.zip(aveSumSplinter).map{
                                       ^
<console>:80: error: value - is not a member of Any
             case (u,v) => u-v
```
