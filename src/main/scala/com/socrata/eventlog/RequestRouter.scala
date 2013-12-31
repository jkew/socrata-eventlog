package com.socrata.eventlog

import com.twitter.finagle.http.{Response, Request}
import com.twitter.finagle.Service
import com.twitter.finagle.http.service.RoutingService

/**
 * Router for handling Request objects; with a mixture of methods and Paths
 * based off of: https://gist.github.com/soheilhy/2927765
 *
 * See RoutingService
 */
object RequestRouter {
  def byRequest[REQUEST](routes: PartialFunction[Request, Service[REQUEST, Response]]) =
    new RoutingService(
      new PartialFunction[Request, Service[REQUEST, Response]] {
        def apply(request: Request)       = routes(request)
        def isDefinedAt(request: Request) = routes.isDefinedAt(request);
      })
}
