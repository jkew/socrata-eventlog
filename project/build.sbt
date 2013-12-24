
resolvers += "socrata maven" at "https://repo.socrata.com/artifactory/libs-release"

resolvers += "twitter-maven" at "http://maven.twttr.com"

resolvers += Resolver.url("socrata ivy", new URL("https://repo.socrata.com/artifactory/ivy-libs-release"))(Resolver.ivyStylePatterns)


externalResolvers <<= resolvers map { rs =>
  Resolver.withDefaultResolvers(rs, mavenCentral = true)
}

addSbtPlugin("com.socrata" % "socrata-sbt" % "0.2.3")
