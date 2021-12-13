FROM coady/pylucene

USER root

WORKDIR /data

RUN pip install simplemma

CMD ["python", "searcher.py"]