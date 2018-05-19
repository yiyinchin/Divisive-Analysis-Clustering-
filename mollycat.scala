 var loop1 = 0
    var loop2 = 0
    var loop3 = 0
    var loop4 = 0
    var loop5 = 0
    var loop6 = 0
    var loop7 = 0
    var loop8 = 0
    var loop9 = 0
    var loop10 = 0
    var loop11 = 0
    var loop12 = 0
    var loop13 = 0
    var loop14 = 0
    var loop15 = 0
    var loop16 = 0
    var loop17 = 0
    var loop18 = 0
    var loop19 = 0

    val whileStartTime = System.nanoTime()

    while (found < end) {
      val startTime433 = System.nanoTime()
      //while number of clusters is less than the number of clusters at the end
      //LargeDiam is the cluster with the largest Diameter, so the next to be split
      size = ((largeDiam.map { case (i, value) => i }).collect).length
      val inTimeSeconds436 = (System.nanoTime() - startTime433) / 1e9
      println("Time Taken for line 436 : " + inTimeSeconds436)
      if (size == 2) {
        val ifSize2StartTime = System.nanoTime()
        diameters = diameter(largeDiam)
        val keys = (largeDiam.map { case (i, value) => i }).collect
        val checkOrder = secOrder.exists(_.sameElements(keys))
        val soughtObject = secOrder.filter(_.sameElements(keys))
        val indexOldSplinters = secOrder.indexOf(soughtObject(0))
        var firstElement = (ArrayBuffer(keys(0))).toArray
        var secondElement = (ArrayBuffer(keys(1))).toArray
        val inTimeSeconds442 = (System.nanoTime() - ifSize2StartTime) / 1e9
        println("Time Taken for line 442-448 : " + inTimeSeconds442)
        if(checkOrder){ // if (checkOrder == true)
          if(firstElement.min < secondElement.min) {
            val ifCheckStartTime = System.nanoTime()
            clustOrder.update(indexOldSplinters, firstElement)
            clustOrder.insert(indexOldSplinters + 1, secondElement)
            secOrder.update(indexOldSplinters, firstElement)
            secOrder.insert(indexOldSplinters + 1, secondElement)
            val inTimeSeconds456 = (System.nanoTime() - ifCheckStartTime) / 1e9
            println("Time Taken for line 456-459 : " + inTimeSeconds456)
            loop3 = loop3 + 1
          }
          else{
            val elseCheckStartTime = System.nanoTime()
            clustOrder.update(indexOldSplinters, secondElement)
            clustOrder.insert(indexOldSplinters + 1, firstElement)
            secOrder.update(indexOldSplinters, secondElement)
            secOrder.insert(indexOldSplinters + 1, firstElement)
            val inTimeSeconds466 = (System.nanoTime() - elseCheckStartTime) / 1e9
            println("Time Taken for line 466-469  : " + inTimeSeconds466)
            loop4 = loop4 + 1
          }
          loop5 = loop5 + 1
        }
        val lengthLeft = (clustOrder.map{ case(i) => i.length}).toArray
        val cosmos = (lengthLeft.take(indexOldSplinters+1)).sum
        clustHei.update(cosmos, diameters)
        val inTimeSeconds478 = (System.nanoTime() - ifSize2StartTime) / 1e9
        println("Time Taken for line 442-478 : " + inTimeSeconds478)
        //get the remaining heights of the clusters
        if (stack.isEmpty) {
          val timeLine484 = System.nanoTime()
          val inTimeSeconds484 = (System.nanoTime() - timeLine484) / 1e9
          println("Time Taken for line 484-485 : " + inTimeSeconds484)
          loop6 = loop6 + 1
        } else {
          val timeLine495 = System.nanoTime()
          val reKeys = stack.pop
          val remainA = keyedMat.filter { case (i, value) => reKeys.exists(_ == i) }
          val remainClustA = remainA.map { case (i, value) => (i, reKeys map value) }
          largeDiam = remainClustA
          inputKey = (remainClustA.map { case (i, value) => i }).collect
          val inTimeSeconds495 = (System.nanoTime() - timeLine495) / 1e9
          println("Time Taken for line 496-501 : " + inTimeSeconds495)
          loop7 = loop7 + 1
        }
        val inTimeSeconds507 = (System.nanoTime() - ifSize2StartTime) / 1e9
        println("Time Taken for line 447-505 : " + inTimeSeconds507)
        loop1 = loop1 + 1
      }
      else {
        val elseNot2StartTime = System.nanoTime()
        //Find the splinter element from the cluster
        splinterIndex = keyRMAD(largeDiam)
        keyA += splinterIndex
        splinterKeys = keyA.toArray
        val inTimeSeconds518 = (System.nanoTime() - elseNot2StartTime) / 1e9
        println("Time Taken for line 516-518: " + inTimeSeconds518)
        while (splinterIndex != -1) {
          val SINot1StartTime = System.nanoTime()
          // Find the rest of the splinter group from the cluster

          val splintObjNot1StartTime = System.nanoTime()
          val splintObj = objSplinter(keyedMat, splinterKeys, inputKey)
          val inTimeSeconds429 = (System.nanoTime() - splintObjNot1StartTime) / 1e9
          println("Time Taken line 427: " + inTimeSeconds429)

          val remainObjNot1StartTime = System.nanoTime()
          val remainObj = objRemains(keyedMat, splinterKeys, inputKey)
          val inTimeSeconds431 = (System.nanoTime() - remainObjNot1StartTime) / 1e9
          println("Time Taken line 432: " + inTimeSeconds431)

          val splinterIndexNot1StartTime = System.nanoTime()
          splinterIndex = diffAD(remainObj, splintObj) // largest positive splinter from the cluster
          val inTimeSeconds436 = (System.nanoTime() - splinterIndexNot1StartTime) / 1e9
          println("Time Taken line 437: " + inTimeSeconds436)

          val inTimeSeconds529 = (System.nanoTime() - SINot1StartTime) / 1e9
          println("Time Taken line 527-529: " + inTimeSeconds529)

          if (splinterIndex != -1) {
            val splintIntIf = System.nanoTime()

            // add to the set of splinter keys
            keyA += splinterIndex
            splinterKeys = keyA.toArray

            val inTimeSeconds539 = (System.nanoTime() - splintIntIf) / 1e9
            println("Time Taken for line 538 & 539: " + inTimeSeconds539)
            loop8 = loop8 + 1
          }
          else {
            val SI1StartTime = System.nanoTime()
            // -1, so no more splinter elements from this cluster
            // record current split and set up next splinter
            //save the remaining group of the cluster and record the current height
            val remain = groups(splinterKeys, inputKey, keyedMat)
            val height1 = height(splintObj, remainObj)

            val inputKeyPrev = inputKey
            val keyS = splinterKeys//get the splinterIndex groups
            val keyR = inputKey diff keyS // get the remain groups
            val sKey = keyS.sorted
            val sLength = keyS.length
            val rLength = keyR.length

            val inTimeSeconds560 = (System.nanoTime() - SI1StartTime) / 1e9
            println("Time Taken for line 547-560: " + inTimeSeconds560)

            if(clustOrder.isEmpty){
              val isEmptyStartTime = System.nanoTime()

              val sKey = keyS.sorted
              if(keyS.min < keyR.min){
                clustOrder += keyS
                clustOrder += keyR
                secOrder += sKey
                secOrder += keyR
                clustHei.update(sLength, height1)
                loop10 = loop10 + 1
              } else {
                clustOrder += keyR
                clustOrder += keyS
                secOrder += keyR
                secOrder += sKey
                clustHei.update(rLength, height1)
                loop11 = loop11 + 1
              }
              val inTimeSeconds582 = (System.nanoTime() - isEmptyStartTime) / 1e9
              println("Time Taken for 567-582" + inTimeSeconds582)
              loop9 = loop9 + 1
            }
            else {
              val notEmptyStartTime = System.nanoTime()

              val checkOrder = secOrder.exists(_.sameElements(inputKeyPrev))
              val soughtObject = secOrder.filter(_.sameElements(inputKeyPrev))
              val indexOldSplinters = secOrder.indexOf(soughtObject(0))

              val inTimeSeconds591 = (System.nanoTime() - notEmptyStartTime) / 1e9
              println("Time Taken for 589-591 " + inTimeSeconds591)

              if(checkOrder){ // if (checkOrder == true)
                // keyS is not ordered, so sKey used when inserting
                val checkOrderStartTime = System.nanoTime()
                if(keyR.min < keyS.min) {
                  clustOrder.update(indexOldSplinters, keyR)
                  clustOrder.insert(indexOldSplinters + 1, keyS)
                  secOrder.update(indexOldSplinters, keyR)
                  secOrder.insert(indexOldSplinters + 1, sKey)
                  loop13 = loop13 + 1
                }
                else{
                  clustOrder.update(indexOldSplinters, keyS)
                  clustOrder.insert(indexOldSplinters + 1, keyR)
                  secOrder.update(indexOldSplinters, sKey)
                  secOrder.insert(indexOldSplinters + 1, keyR)
                  loop14 = loop14 + 1
                }
                val inTimeSeconds564 = (System.nanoTime() - checkOrderStartTime) / 1e9
                println("Time Taken for line564-575: " + inTimeSeconds564)
                loop12 = loop12 + 1
              }
              val lengthLeft = (clustOrder.map{ case(i) => i.length}).toArray
              val cosmos = (lengthLeft.take(indexOldSplinters+1)).sum
              clustHei.update(cosmos, height1)

              val inTimeSeconds634 = (System.nanoTime() - notEmptyStartTime) / 1e9
              println("Time Taken for line 600-634 : " + inTimeSeconds634)
              loop15 = loop15 + 1
            }
            //Find the next initial cluster
            largeDiam = largestDiam(keyedMat, remainObj, splinterKeys)
            val inputKey1 = largeDiam.map { case (i, value) => i }
            inputKey = inputKey1.collect

            val keyRemain = inputKeyPrev diff inputKey
            val numKeys = keyRemain.length
            if (numKeys > 1) {
              stack.push(keyRemain)
              loop16 = loop16 + 1
            }
            keyA = ArrayBuffer()
            val inTimeSeconds653 = (System.nanoTime() - SI1StartTime) / 1e9
            println("Time Taken for line 558 - 653 : " + inTimeSeconds653)
            loop17 = loop17 + 1
          }
          val inTimeSeconds657 = (System.nanoTime() - SINot1StartTime) / 1e9
          println("Time Taken for line 557-657 : " + inTimeSeconds657)
          loop18 = loop18 + 1
        }
        val inTimeSeconds661 = (System.nanoTime() - elseNot2StartTime) / 1e9
        println("Time Taken for line 535-661: " + inTimeSeconds661)
        loop2 = loop2 + 1
      }
      found = found + 1
      println(" ")
      loop19 = loop19 + 1
    }
    val inTimeSeconds = (System.nanoTime() - whileStartTime) / 1e9
    println("Time Taken: " + inTimeSeconds)
    println("The orders: " + stringOf(clustOrder))
    println("Banner: " + stringOf(clustHei))
    val dC = divisiveCoef(clustHei)
