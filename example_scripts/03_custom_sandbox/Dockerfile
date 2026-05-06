FROM aisiuk/inspect-tool-support:latest
COPY create_subrepo.sh .
RUN ./create_subrepo.sh && rm create_subrepo.sh
WORKDIR /subrepo
CMD ["tail", "-f", "/dev/null"]
