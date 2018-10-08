package es.us.idea.adt.dsl

import es.us.idea.adt.data.Data

trait Container {
  def build(): Data
}