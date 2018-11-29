package es.us.idea.adt.dsl

import es.us.idea.adt.data.functions.ADTDataFunction

trait Modifiable extends Container {

  def /(f: ADTDataFunction) = new DataModifierContainer(this)(f)

}
