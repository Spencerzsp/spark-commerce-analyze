package conf

import org.apache.commons.configuration2.{FileBasedConfiguration, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters

/**
  * 配置管理器
  */
object ConfigurationManager {

  def main(args: Array[String]): Unit = {

  }

  private val params = new Parameters()
  private val builder: FileBasedConfigurationBuilder[FileBasedConfiguration] = new FileBasedConfigurationBuilder[FileBasedConfiguration](classOf[PropertiesConfiguration])
    .configure(params.properties().setFileName("D:\\IdeaProjects\\spark-commerce-analyze\\commons\\src\\main\\resources\\commerce.properties"))

  val config: FileBasedConfiguration = builder.getConfiguration

}
