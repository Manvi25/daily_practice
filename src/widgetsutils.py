# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.dropdown("fruits", "apple", ["grapes", "apple", "papaya"])

# COMMAND ----------

dbutils.widgets.multiselect("fruit_multiselect",  "apple",["apple", "grapes", "papaya"])

# COMMAND ----------

dbutils.widgets.combobox(name='fruits', defaultValue='apple',choices=['apple','grapes','papaya'],label='fruits combo box')

# COMMAND ----------

dbutils.widgets.text(name='fruits tb',defaultValue='apple',label='fruits textbox')

# COMMAND ----------

dbutils.widgets.get('fruits')

# COMMAND ----------

dbutils.widgets.getArgument('fruitsCB','error:this widget is not defined')
