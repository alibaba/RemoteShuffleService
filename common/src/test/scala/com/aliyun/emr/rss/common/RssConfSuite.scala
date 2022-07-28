package com.aliyun.emr.rss.common

import com.aliyun.emr.RssFunSuite
import com.aliyun.emr.rss.common.RssConf.{haMasterHosts, masterPort}
import com.aliyun.emr.rss.common.protocol.StorageHint

class RssConfSuite extends RssFunSuite{

  test("issue 26: rss.master.address support multi host") {
    val conf = new RssConf()
    conf.set("rss.master.address", "localhost1:9097,localhost2:9097")
    assert("localhost1,localhost2" == haMasterHosts(conf))
    assert(9097 == masterPort(conf))
  }

  test("basedir test") {
    val conf = new RssConf()
    val defaultMaxUsableSpace = 1024L * 1024 * 1024 * 1024 * 1024
    conf.set("rss.worker.base.dirs", "/mnt/disk1")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 1)
    assert(parsedDirs.head._4 == StorageHint.Type.HDD)
    assert(parsedDirs.head._2 == defaultMaxUsableSpace)
  }

  test("basedir test2") {
    val conf = new RssConf()
    val defaultMaxUsableSpace = 1024L * 1024 * 1024 * 1024 * 1024
    conf.set("rss.worker.base.dirs", "/mnt/disk1:disktype=SSD:capacity=10g")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 8)
    assert(parsedDirs.head._4 == StorageHint.Type.SSD)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("basedir test3") {
    val conf = new RssConf()
    conf.set("rss.worker.base.dirs", "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3")
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 1)
    assert(parsedDirs.head._3 == 3)
    assert(parsedDirs.head._4 == StorageHint.Type.SSD)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)
  }

  test("basedir test4") {
    val conf = new RssConf()
    conf.set(
      "rss.worker.base.dirs",
      "/mnt/disk1:disktype=SSD:capacity=10g:flushthread=3," +
        "/mnt/disk2:disktype=HDD:capacity=15g:flushthread=7"
    )
    val parsedDirs = RssConf.workerBaseDirs(conf)
    assert(parsedDirs.size == 2)
    assert(parsedDirs.head._1 == "/mnt/disk1")
    assert(parsedDirs.head._3 == 3)
    assert(parsedDirs.head._4 == StorageHint.Type.SSD)
    assert(parsedDirs.head._2 == 10 * 1024 * 1024 * 1024L)

    assert(parsedDirs(1)._1 == "/mnt/disk2")
    assert(parsedDirs(1)._3 == 7)
    assert(parsedDirs(1)._4 == StorageHint.Type.HDD)
    assert(parsedDirs(1)._2 == 15 * 1024 * 1024 * 1024L)
  }
}
