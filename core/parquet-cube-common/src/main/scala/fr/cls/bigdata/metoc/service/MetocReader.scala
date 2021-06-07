package fr.cls.bigdata.metoc.service

import fr.cls.bigdata.georef.model.VariableRef
import fr.cls.bigdata.georef.utils.Rounding
import fr.cls.bigdata.hadoop.model.PathWithFileSystem
import fr.cls.bigdata.resource.Resource


trait MetocReader[A] {
  /**
    * Reads a Metoc data source file and returns a managed MetocDatasetAccess
    *
    * @param file the metoc file
    * @param rounding the rouning method
    * @param variablesToExclude a set of variables to exclude
    * @return
    */
  def read(file: PathWithFileSystem, rounding: Rounding, variablesToExclude: Set[String]): Resource[MetocDatasetAccess[A]]
}
