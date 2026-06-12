import numpy as np
import pickle
from sklearn.linear_model import LogisticRegression

# Fake training data (dimension = 11 features temps réel)
X = np.random.rand(1000, 11)
y = (X[:, 3] + X[:, 7] > 1.2).astype(int)  # logique simple

model = LogisticRegression()
model.fit(X, y)

with open("ml/fraud_model.pkl", "wb") as f:
    pickle.dump(model, f)

print("✅ Dummy fraud model saved")