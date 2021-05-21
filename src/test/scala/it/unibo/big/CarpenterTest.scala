package it.unibo.big

import it.unibo.big.Carpenter.Itemset
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.collection.mutable
import scala.io.{Codec, Source}

class CarpenterTest {
    var data: Iterable[Set[Any]] = List()
    var stats: mutable.Map[String, Long] = mutable.Map()
    var res: Set[Itemset] = Set()
    implicit val codec: Codec = Codec("UTF-8")

    @BeforeEach def setup(): Unit = {
        stats = mutable.Map()
        data = List()
        res = Set()
    }

    @AfterEach def cleanup(): Unit = {
        println("Time(ms): " + stats("time(ms)") + " Iterations: " + stats("iterations"))
    }

    @Test def testCarpenter1(): Unit = {
        data = List((0 to 2).map(_.toString).toSet, (0 to 1).map(_.toString).toSet)
        assertEquals(Set(Itemset(Set("0", "1"), 2), Itemset(Set("0", "1", "2"), 1)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter2(): Unit = {
        data = List((0 to 2).map(_.toString).toSet, (0 to 2).map(_.toString).toSet)
        assertEquals(Set(Itemset(Set("0", "1", "2"), 2)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter3(): Unit = {
        data = List(Set(1, 2, 3), Set(1, 2), Set(3))
        assertEquals(Set(Itemset(Set(1, 2, 3), 1), Itemset(Set(1, 2), 2), Itemset(Set(3), 2)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter4(): Unit = {
        data = List(Set(1, 3, 4), Set(2, 3, 5), Set(1, 2, 3, 5), Set(2, 5), Set(1, 2, 3, 5))
        assertEquals(Set(Itemset(Set(3), 4), Itemset(Set(1, 3), 3), Itemset(Set(1, 3, 4), 1), Itemset(Set(2, 5), 4), Itemset(Set(2, 3, 5), 3), Itemset(Set(1, 2, 3, 5), 2)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter5(): Unit = {
        data = List(Set(2, 3, 4), Set(2, 3, 5), Set(3, 4, 5, 6))
        assertEquals(Set(Itemset(Set(2, 3, 4), 1), Itemset(Set(2, 3, 5), 1), Itemset(Set(3, 4, 5, 6), 1), Itemset(Set(2, 3), 2), Itemset(Set(3, 4), 2), Itemset(Set(3, 5), 2), Itemset(Set(3), 3)),
            Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter6(): Unit = {
        data = Vector(Set(1, 2, 3), Set(1, 2, 3), Set(1, 2, 3))
        assertEquals(Set(Itemset(Set(1, 2, 3), 3)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter7(): Unit = {
        data = Vector(Set(1, 2, 3), Set(2, 3, 4), Set(2, 3, 5), Set(2, 3, 6))
        assertEquals(Set(Itemset(Set(2, 3), 4)), Carpenter.run(data, 2, stats).toSet)
    }

    @Test def testCarpenter8(): Unit = {
        data = Vector(Set(1, 2, 3), Set(1, 2, 3), Set(2, 3))
        assertEquals(Set(Itemset(Set(2, 3), 3), Itemset(Set(1, 2, 3), 2)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter9(): Unit = {
        data = Vector(Set(1, 2, 3), Set(2, 3), Set(1, 2, 3))
        assertEquals(Set(Itemset(Set(2, 3), 3), Itemset(Set(1, 2, 3), 2)), Carpenter.run(data, 1, stats).toSet)
    }

    @Test def testCarpenter10(): Unit = {
        data = Source.fromFile("src/main/resources/closed0_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(7, Carpenter.run(data, 1, stats).toSet.size)
    }

    @Test def testCarpenter11(): Unit = {
        data = Source.fromFile("src/main/resources/closed1_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(6, Carpenter.run(data, 1, stats).toSet.size)
    }

    @Test def testCarpenter12(): Unit = {
        data = Source.fromFile("src/main/resources/closed2_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(5, Carpenter.run(data, 1, stats).toSet.size)
    }

    @Test def testCarpenter13(): Unit = {
        data = Source.fromFile("src/main/resources/closed4_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(7, Carpenter.run(data, 1, stats).toSet.size)
    }

    @Test def testCarpenter14(): Unit = {
        data = Source.fromFile("src/main/resources/closed4_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(7, Carpenter.run(data, 3, stats).toSet.size)
    }

    @Test def testCarpenter15(): Unit = {
        data = Source.fromFile("src/main/resources/closed4_trans.txt").getLines.map(l => l.split(" ").map(_.trim.asInstanceOf[Any]).toSet).toList
        assertEquals(5, Carpenter.run(data, 5, stats).toSet.size)
    }
}
