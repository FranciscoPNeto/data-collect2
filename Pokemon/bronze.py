# %%

df = spark.read.json("data/pokemon/")
df.display()
# %%