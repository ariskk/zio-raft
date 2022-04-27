import sbt.Command

/**
 * @author 梦境迷离
 * @since 2022/1/15
 * @version 1.0
 */
object Commands {

  val FmtSbtCommand = Command.command("fmt")(state => "scalafmtSbt" :: "scalafmtAll" :: state)

  val FixSbtCommand = Command.command("fix")(state =>
    "scalafixEnable" :: "scalafixAll RemoveUnused" :: "scalafixAll SortImports" :: state
  )

  val CheckSbtCommand =
    Command.command("check")(state => "scalafixEnable" :: "scalafixAll" :: "scalafmtCheckAll" :: state)

  val value: Seq[Command] = Seq(
    FmtSbtCommand,
    FixSbtCommand,
    CheckSbtCommand
  )

}
