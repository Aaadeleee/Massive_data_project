import pandas as pd
import matplotlib.pyplot as plt

# Load CSV
#df = pd.read_csv("out/conc.csv")
df = pd.read_csv("out/post.csv")
#df = pd.read_csv("out/fanout.csv")

# Convert "266ms" → 266
df["AVG_TIME"] = df["AVG_TIME"].str.replace("ms", "").astype(int)

# Convert ms → seconds
df["SECONDS"] = df["AVG_TIME"] / 1000

# Group by PARAM → mean + std
stats = df.groupby("PARAM")["SECONDS"].agg(["mean", "std"])

# Prepare plot
plt.figure(figsize=(10, 6))

plt.bar(stats.index.astype(str), stats["mean"],
        yerr=stats["std"],
        capsize=5,
        alpha=0.6,
        edgecolor="black")

# Labels
plt.xlabel("Number of posts per user")
plt.ylabel("Average Time per Request (s)")
#plt.title("Average Request Time by Concurrency Level")
plt.title("Average Request Time by Posts Number\n(50 concurrent requests, 20 followers)")
#plt.title("Average Request Time by Fanout Size\n(50 concurrent requests, 100 posts per users)")


plt.grid(axis="y", linestyle="--", alpha=0.4)
plt.tight_layout()

#plt.savefig("conc.png", dpi=300)
plt.savefig("post.png", dpi=300)
#plt.savefig("fanout.png", dpi=300)
plt.show()
