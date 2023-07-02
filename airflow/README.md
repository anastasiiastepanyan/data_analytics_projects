# Working with Apache Airflow, a platform to programmatically author, schedule, and monitor workflows.

## Projects description

 1. game_sales_analytics_dag.py

    A DAG is a Directed Acyclic Graph.

    The script sets a DAG with task decorators which answers following questions based on provided data:

      - What game was the best-selling this year worldwide?
      - What game genres were the best-selling in Europe?
      - Which platform had the most games that sold over a million copies in North America?
      - Which publisher has the highest average sales in Japan?
      - How many games have sold better in Europe than in Japan?

    In case of successful completion of the DAG, the message "Well done! Dag {dag_id} completed on {date}" is sent by the telegram bot.
