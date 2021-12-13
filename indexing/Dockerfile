FROM coady/pylucene

USER root

WORKDIR /data

RUN pip install jupyter simplemma

EXPOSE 8888

CMD ["jupyter-notebook", "--allow-root", "--ip=0.0.0.0", "--port=8888"]