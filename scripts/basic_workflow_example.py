""" Model selection via RMSE
This scripts runs the workflow defined in
https://github.com/phenology/phenologyX/issues/4

Required dependencies can be installed using
the conda environment `springtime`, see
https://github.com/phenology/phenologyX#springtime

usage:
mamba activate springtime
python basic_workflow_example.py

output: RMSE_comparison.csv RMSE_comparison.jpg

"""
from springtime import build_workflow, run_workflow
import pandas as pd
import seaborn

# dataset and phenophases to test
dataset = "vaccinium"
phenophases = ["budburst", "flowers"]

# train/test strategy
train_test_strategy= "ShuffleSplit"

# pyPhenology models to test
models_to_test = ["ThermalTime", "Linear"]

# sklearn ML models to test
models_to_test.append("LinearRegression")

# model selection strategy
metric_name = "RMSE"

# Run the workflow
## store the results
all_results = {
    "dataset": [],
    "phenophase": [],
    "model": [],
    "rmse": [],
}

for phenophase in phenophases:
    for model_name in models_to_test:
        print(
            f"Running workflow for data {phenophase} "
            f"and model {model_name}"
            )
        ## Create options for workflow
        options = {
            "dataset": dataset,
            "phenophase": phenophase,
            "model_name": model_name,
            "train_test_strategy": train_test_strategy,
            "metric_name": metric_name,
        }
        workflow_name = f"{dataset}_{phenophase}_{model_name}"
        workflow = build_workflow(options, name=workflow_name)
        results = run_workflow(workflow)

        all_results['rmse'].append(results["metric_value"])
        all_results['dataset'].append(dataset)
        all_results['phenophase'].append(phenophase)
        all_results['model'].append(model_name)

# save results
results_df_name = "RMSE_comparison.csv"
results_df = pd.DataFrame.from_dict(all_results)
results_df.to_csv(results_df_name, sep=',')

# plot results
results_plot_name = "RMSE_comparison.jpg"
ax = seaborn.barplot(results_df, x="model", y="rmse", hue='phenophase')
ax.figure.savefig(results_plot_name)

print(f"Runs are done! check {results_df_name} and {results_plot_name}")
