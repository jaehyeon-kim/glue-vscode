pyspark:
	@echo Run pyspark as a root
	sudo su -c '/home/aws-glue-libs/bin/gluepyspark'

spark-submit:
	@echo Run spark-submit as a root, source $(app)
	sudo su -c '/home/aws-glue-libs/bin/gluesparksubmit $(app)'