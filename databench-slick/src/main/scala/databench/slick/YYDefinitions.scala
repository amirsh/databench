package scala.slick.yy.test

import scala.slick.yy.Entity

object YYDefinitions {
  @Entity("SLICK_ACCOUNT") case class Account(id: Int, balance: Int, transfers: String) {
  	def transferValues =
          transfers.split(',').tail.map(Integer.parseInt(_).intValue)
  }
}