all: pyspark

pyspark:
	@echo Run pyspark as root
	sudo su -c '/home/aws-glue-libs/bin/gluepyspark'

spark-submit:
	@echo Run spark-submit as root, application - $(app)
	sudo su -c '/home/aws-glue-libs/bin/gluesparksubmit $(app)'

pytest:
	@echo Run pytest as root
	sudo su -c '/home/aws-glue-libs/bin/gluepytest -svv'

.PHONY: all pyspark spark-submit pyspark