.PHONY: debug
debug:
	cd .. && \
	rm -f result.zip && \
	zip -r result.zip lindorm-tsdb-contest-java debug jvm

.PHONY: prod
prod:
	cd .. && \
	rm -f result.zip && \
	rm -f jvm && \
	cp ./lindorm-tsdb-contest-java/jvm jvm && \
	zip -r result.zip lindorm-tsdb-contest-java jvm
