# syntax=docker/dockerfile:1
# One Dockerfile, four stages:
#   dashboard  builds the React app
#   build      compiles every Go binary and the Lambda zip
#   app        small runtime image with all services and the dashboard files
#   infra      Terraform + the Lambda zip, applied to LocalStack by `make up`

FROM node:24-alpine AS dashboard
WORKDIR /dash
COPY dashboard/package.json dashboard/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ \
      ./cmd/ingestor ./cmd/normaliser ./cmd/metrics-engine ./cmd/risk-engine ./cmd/archiver \
      ./cmd/metrics-sink ./cmd/alert-bridge ./cmd/api-gateway ./cmd/replayer ./cmd/backtester
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    apk add --no-cache zip >/dev/null && mkdir -p /lambda && \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -tags lambda.norpc \
      -o /lambda/bootstrap ./cmd/alert-handler && \
    cd /lambda && zip -q alert-handler.zip bootstrap

FROM gcr.io/distroless/static-debian12:nonroot AS app
COPY --from=build /out/ /app/
COPY --from=dashboard /dash/dist/ /app/dashboard/
COPY deploy/rules.yaml /etc/tickstream/rules.yaml
USER nonroot

FROM hashicorp/terraform:1.16 AS infra
WORKDIR /infra
COPY infra/*.tf infra/*.tfvars infra/.terraform.lock.hcl ./
# Download the AWS provider at build time so `make up` does not.
RUN terraform init -input=false -backend=false >/dev/null
COPY --from=build /lambda/alert-handler.zip /build/alert-handler.zip
ARG TARGETARCH
ENV LAMBDA_ARCH=${TARGETARCH}
ENTRYPOINT ["/bin/sh", "-c", "\
  arch=arm64; [ \"$LAMBDA_ARCH\" = amd64 ] && arch=x86_64; \
  terraform apply -input=false -auto-approve -state=/state/terraform.tfstate \
    -var-file=local.tfvars -var localstack_endpoint=http://localstack:4566 \
    -var lambda_zip=/build/alert-handler.zip -var lambda_architecture=$arch \
    -var alert_webhook_url=\"$ALERT_WEBHOOK_URL\""]
