package mf.dabi.pso.techIndicatorTradingSystem.utils

import mf.dabi.pso.techIndicatorTradingSystem.settings.Sparkable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileSystemSuite extends Sparkable {

  lazy val hdfsconf: Configuration = sc.hadoopConfiguration

  def fs: FileSystem = FileSystem.get(hdfsconf)

  private def pathfs(path: String) = new Path(path)

  def ls(path: String): Array[String] = {
    val a = pathfs(path)
    val list = fs.listStatus(a)
    list.map(file => file.getPath.toUri.getRawPath)
  }

  def fileName(path: String): Array[String] = ls(path).map(file => file.split("/").last)
}
