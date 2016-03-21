docker rm hadoop
docker run -it -v ~/projects/WikiHadoop/data/input/enwiki-20160204-pages-articles-multistream.xml:/enwiki-20160204-pages-articles-multistream.xml -v ~/projects/WikiHadoop/target:/WikiHadoop/target --name hadoop sequenceiq/hadoop-docker:latest /etc/bootstrap.sh -bash
