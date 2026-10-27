import pickle

model_file = './cohorts/2026/homework/05-deployment/pipeline.bin'

with open(model_file, 'rb') as f_in:
    dv, model = pickle.load(f_in)

customer = {
  "lead_source": "paid_ads",
  "industry": "technology",
  "employment_status": "employed",
  "location": "north_america",
  "number_of_courses_viewed": 2,
  "annual_income": 79276.0,
  "interaction_count": 4,
  "lead_score": 0.41
}

X = dv.transform([customer])
y_pred = model.predict_proba(X)[0, 1]

print(float(y_pred))


