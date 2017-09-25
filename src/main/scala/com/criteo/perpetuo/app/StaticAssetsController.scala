package com.criteo.perpetuo.app

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import javax.activation.MimetypesFileTypeMap

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.http.Controller
import org.apache.commons.io.FilenameUtils

/**
  * Serve static assets
  */
class StaticAssetsController(rootPaths: Seq[String]) extends Controller {

  class Resolver(rootNames: Seq[String]) {

    private val roots: Seq[Either[File, String]] = {
      val dirPrefix = "dir://"
      val pacPrefix = "package://"

      val eitherRoots = rootNames.map { root =>
        if (root.startsWith(dirPrefix)) {
          Left(new File(root.substring(dirPrefix.length)))
        } else if (root.startsWith(pacPrefix)) {
          Right(root.substring(pacPrefix.length))
        } else
          throw new IllegalArgumentException(s"unsupported root format: $root")
      }
      if (eitherRoots.nonEmpty) eitherRoots else Seq(Right(""))
    }

    private val extMap = new MimetypesFileTypeMap()

    def getInputStream(path: String): Option[InputStream] =
      if (path.endsWith("/"))
        None
      else {
        val slashPath = if (path.startsWith("/")) path else s"/$path"

        roots.toStream.flatMap({
          case Left(root) =>
            tryGetFileInputStream(root, slashPath)
          case Right(root) =>
            tryGetResourceInputStream(root, slashPath)
        }).headOption
      }

    def getContentType(file: String) =
      extMap.getContentType('.' + FilenameUtils.getExtension(file))

    private def tryGetFileInputStream(root: File, path: String): Option[BufferedInputStream] = {
      val file = new File(root, path)
      if (file.exists)
        Some(new BufferedInputStream(new FileInputStream(file)))
      else
        None
    }

    private def tryGetResourceInputStream(root: String, path: String): Option[BufferedInputStream] = {
      for {
        is <- Option(getClass.getResourceAsStream(s"$root$path"))
        bis = new BufferedInputStream(is)
        if 0 < bis.available
      } yield bis
    }
  }

  val resolver = new Resolver(rootPaths)

  private def file(path: String): Response =
    resolver.getInputStream(path).map(inputStream =>
      response
        .ok
        .contentType(resolver.getContentType(path))
        .body(inputStream)
    ).getOrElse(
      response.notFound.plain(s"$path not found")
    )

  Array(
    "/manifest.json",
    "/logo-32x32.png",
    "/src/:*",
    "/bower_components/:*"
  ).foreach(uri => {
    get(uri) { request: Request =>
      file(request.uri)
    }
  })

  get("/:*") { _: Request =>
    file("index.html")
  }

}
