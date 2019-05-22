/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.typesafe.sbt.SbtGit._
import sbt._

object Versioning {

  val snapshotSuffix = "-SNAPSHOT"
  val releasedVersion = """^r?([0-9\.]+)$""".r
  val releasedCandidateVersion = """^r?([0-9\.]+-rc\d+)$""".r
  val betaVersion = """^r?([0-9\.]+-beta\d+)$""".r
  val snapshotVersion = """^r?[0-9\.]+(.*)$""".r

  def settings(baseVersion: String): Seq[Def.Setting[_]] = Seq(
    git.baseVersion := baseVersion,
    git.uncommittedSignifier := None,
    git.useGitDescribe := true,
    git.formattedShaVersion := git.gitHeadCommit.value map(sha => s"$baseVersion-${sha take 7}$snapshotSuffix"),
    git.gitTagToVersionNumber := {
      case releasedVersion(v) => Some(v)
      case releasedCandidateVersion(rc) => Some(rc)
      case betaVersion(beta) => Some(beta)
      case snapshotVersion(v) => Some(s"$baseVersion$v$snapshotSuffix")
      case _ => None
    }
  )

}