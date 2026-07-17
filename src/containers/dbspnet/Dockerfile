# DbspNet engine builder — clones the dbsp-net repo (a .NET DBSP implementation) at a
# pinned commit and builds the DbspNet control service (the pipeline-manager analogue).
# Follows the same from-source pattern as the other built engines (e.g. duckdb-openivm):
# ivm-bench fetches the engine source itself, no sibling checkout needed. Bump the engine
# by overriding DBSPNET_COMMIT (and DBSPNET_REPO for a fork).
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG DBSPNET_REPO=https://github.com/clast-project/dbsp-net.git
ARG DBSPNET_COMMIT=a22c3a24ab631327aa5cc062e868b47da8eb8b22

# git + certs for the clone (+ engineered-wood submodule fetch over https).
RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone "${DBSPNET_REPO}" . \
 && git checkout "${DBSPNET_COMMIT}" \
 && git submodule update --init --recursive
RUN dotnet publish src/DbspNet.Server/DbspNet.Server.csproj -c Release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0
# curl for the compose healthcheck — the aspnet runtime image is Debian-slim and
# does NOT ship curl, so without this the /healthz probe fails with "curl: not
# found" and the container never reports healthy (even when the service is up),
# which cascades into the dbt-server dependency failing to start.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "DbspNet.Server.dll"]
