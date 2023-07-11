# SPDX-FileCopyrightText: 2023 Springtime authors
#
# SPDX-License-Identifier: Apache-2.0

# TODO get nested fit_kwargs into pyfacet release
# Code copied from
# https://github.com/pycaret/pycaret/blob/fa8cb5e800ba2249520e2f57d27c6584c99a2eed/pycaret/internal/pycaret_experiment/supervised_experiment.py#L369
# ruff: noqa: E501
# mypy: ignore-errors

import datetime
import time
import traceback
import warnings
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from pycaret import regression
from pycaret.internal.display import CommonDisplay, DummyDisplay
from pycaret.internal.parallel.parallel_backend import ParallelBackend
from pycaret.internal.validation import is_sklearn_cv_generator
from pycaret.utils.generic import MLUsecase, id_or_display_name


class NestedFitRegressionExperiment(regression.RegressionExperiment):
    def compare_models(
        self,
        include: List[str | Any] | None = None,
        exclude: List[str] | None = None,
        fold: int | Any | None = None,
        round: int = 4,
        cross_validation: bool = True,
        sort: str = "R2",
        n_select: int = 1,
        budget_time: float | None = None,
        turbo: bool = True,
        errors: str = "ignore",
        fit_kwargs: dict | None = None,
        groups: str | Any | None = None,
        experiment_custom_tags: Dict[str, Any] | None = None,
        probability_threshold: Optional[float] = None,
        engine: Dict[str, str] | None = None,
        verbose: bool = True,
        parallel: ParallelBackend | None = None,
        caller_params: Optional[dict] = None,
    ):
        """Same as https://pycaret.readthedocs.io/en/latest/api/regression.html#pycaret.regression.compare_models

        But the fit_kwargs parameter is nested by model name.
        """
        self._check_setup_ran()

        if parallel is not None:
            return self._parallel_compare_models(parallel, caller_params, turbo=turbo)

        # No extra code should be added above this line
        # --------------------------------------------------------------

        function_params_str = ", ".join([f"{k}={v}" for k, v in locals().items()])

        self.logger.info("Initializing compare_models()")
        self.logger.info(f"compare_models({function_params_str})")

        self.logger.info("Checking exceptions")

        if not fit_kwargs:
            fit_kwargs = {}

        # checking error for exclude (string)
        available_estimators = self._all_models

        if include is not None:
            for i in include:
                if isinstance(i, str):
                    if i not in available_estimators:
                        raise ValueError(
                            f"Estimator {i} Not Available. Please see docstring for list of available estimators."
                        )
                elif not hasattr(i, "fit"):
                    raise ValueError(
                        f"Estimator {i} does not have the required fit() method."
                    )

        # include and exclude together check
        if include is not None and exclude is not None:
            raise TypeError(
                "Cannot use exclude parameter when include is used to compare models."
            )

        # checking fold parameter
        if fold is not None and not (
            type(fold) is int or is_sklearn_cv_generator(fold)
        ):
            raise TypeError(
                "fold parameter must be either None, an integer or a scikit-learn compatible CV generator object."
            )

        # checking round parameter
        if type(round) is not int:
            raise TypeError("Round parameter only accepts integer value.")

        # checking budget_time parameter
        if (
            budget_time
            and type(budget_time) is not int
            and type(budget_time) is not float
        ):
            raise TypeError(
                "budget_time parameter only accepts integer or float values."
            )

        # checking sort parameter
        if not (isinstance(sort, str) and (sort == "TT" or sort == "TT (Sec)")):
            sort = self._get_metric_by_name_or_id(sort)
            if sort is None:
                raise ValueError(
                    "Sort method not supported. See docstring for list of available parameters."
                )

        # checking errors parameter
        possible_errors = ["ignore", "raise"]
        if errors not in possible_errors:
            raise ValueError(
                f"errors parameter must be one of: {', '.join(possible_errors)}."
            )

        # checking optimize parameter for multiclass
        if self.is_multiclass:
            if not sort.is_multiclass:
                raise TypeError(
                    f"{sort} metric not supported for multiclass problems. See docstring for list of other optimization parameters."
                )

        """
        ERROR HANDLING ENDS HERE
        """

        if self._ml_usecase != MLUsecase.TIME_SERIES:
            fold = self._get_cv_splitter(fold)

        groups = self._get_groups(groups)

        pd.set_option("display.max_columns", 500)

        self.logger.info("Preparing display monitor")

        len_mod = (
            len({k: v for k, v in self._all_models.items() if v.is_turbo})
            if turbo
            else len(self._all_models)
        )

        if include:
            len_mod = len(include)
        elif exclude:
            len_mod -= len(exclude)

        progress_args = {"max": (4 * len_mod) + 4 + min(len_mod, abs(n_select))}
        master_display_columns = (
            ["Model"]
            + [v.display_name for k, v in self._all_metrics.items()]
            + ["TT (Sec)"]
        )
        master_display = pd.DataFrame(columns=master_display_columns)
        timestampStr = datetime.datetime.now().strftime("%H:%M:%S")
        monitor_rows = [
            ["Initiated", ". . . . . . . . . . . . . . . . . .", timestampStr],
            [
                "Status",
                ". . . . . . . . . . . . . . . . . .",
                "Loading Dependencies",
            ],
            [
                "Estimator",
                ". . . . . . . . . . . . . . . . . .",
                "Compiling Library",
            ],
        ]
        display = (
            DummyDisplay()
            if self._remote
            else CommonDisplay(
                verbose=verbose,
                html_param=self.html_param,
                progress_args=progress_args,
                monitor_rows=monitor_rows,
            )
        )
        if display.can_update_text:
            display.display(master_display, final_display=False)

        input_ml_usecase = self._ml_usecase
        target_ml_usecase = MLUsecase.TIME_SERIES

        np.random.seed(self.seed)

        display.move_progress()

        # defining sort parameter (making Precision equivalent to Prec. )

        if not (isinstance(sort, str) and (sort == "TT" or sort == "TT (Sec)")):
            sort_ascending = not sort.greater_is_better
            sort = id_or_display_name(sort, input_ml_usecase, target_ml_usecase)
        else:
            sort_ascending = True
            sort = "TT (Sec)"

        """
        MONITOR UPDATE STARTS
        """

        display.update_monitor(1, "Loading Estimator")

        """
        MONITOR UPDATE ENDS
        """

        if include:
            model_library = include
        else:
            if turbo:
                model_library = [k for k, v in self._all_models.items() if v.is_turbo]
            else:
                model_library = list(self._all_models.keys())
            if exclude:
                model_library = [x for x in model_library if x not in exclude]

        if self._ml_usecase == MLUsecase.TIME_SERIES:
            if "ensemble_forecaster" in model_library:
                warnings.warn(
                    "Unsupported estimator `ensemble_forecaster` for method `compare_models()`, removing from model_library"
                )
                model_library.remove("ensemble_forecaster")

        display.move_progress()

        # create URI (before loop)
        import secrets

        URI = secrets.token_hex(nbytes=4)

        master_display = None
        master_display_ = None

        total_runtime_start = time.time()
        total_runtime = 0
        over_time_budget = False
        if budget_time and budget_time > 0:
            self.logger.info(f"Time budget is {budget_time} minutes")

        for i, model in enumerate(model_library):
            model_id = (
                model
                if (
                    isinstance(model, str)
                    and all(isinstance(m, str) for m in model_library)
                )
                else str(i)
            )
            model_name = self._get_model_name(model)

            if isinstance(model, str):
                self.logger.info(f"Initializing {model_name}")
            else:
                self.logger.info(f"Initializing custom model {model_name}")

            # run_time
            runtime_start = time.time()
            total_runtime += (runtime_start - total_runtime_start) / 60
            self.logger.info(f"Total runtime is {total_runtime} minutes")
            over_time_budget = (
                budget_time and budget_time > 0 and total_runtime > budget_time
            )
            if over_time_budget:
                self.logger.info(
                    f"Total runtime {total_runtime} is over time budget by {total_runtime - budget_time}, breaking loop"
                )
                break
            total_runtime_start = runtime_start

            """
            MONITOR UPDATE STARTS
            """

            display.update_monitor(2, model_name)

            """
            MONITOR UPDATE ENDS
            """

            self.logger.info(
                "SubProcess create_model() called =================================="
            )
            create_model_args = dict(
                estimator=model,
                system=False,
                verbose=False,
                display=display,
                fold=fold,
                round=round,
                cross_validation=cross_validation,
                # fit_kwargs=fit_kwargs
                # Changed by sverhoeven to make fit_kwargs nested
                fit_kwargs=fit_kwargs.get(model_name, {}),
                groups=groups,
                probability_threshold=probability_threshold,
                refit=False,
            )
            results_columns_to_ignore = ["Object", "runtime", "cutoff"]
            if errors == "raise":
                model, model_fit_time = self._create_model(**create_model_args)
                model_results = self.pull(pop=True)
            else:
                try:
                    model, model_fit_time = self._create_model(**create_model_args)
                    model_results = self.pull(pop=True)
                    assert (
                        np.sum(
                            model_results.drop(
                                results_columns_to_ignore, axis=1, errors="ignore"
                            ).iloc[0]
                        )
                        != 0.0
                    )
                except Exception:
                    self.logger.warning(
                        f"create_model() for {model} raised an exception or returned all 0.0, trying without fit_kwargs:"
                    )
                    self.logger.warning(traceback.format_exc())
                    try:
                        model, model_fit_time = self._create_model(**create_model_args)
                        model_results = self.pull(pop=True)
                        assert (
                            np.sum(
                                model_results.drop(
                                    results_columns_to_ignore, axis=1, errors="ignore"
                                ).iloc[0]
                            )
                            != 0.0
                        )
                    except Exception:
                        self.logger.error(
                            f"create_model() for {model} raised an exception or returned all 0.0:"
                        )
                        self.logger.error(traceback.format_exc())
                        continue
            self.logger.info(
                "SubProcess create_model() end =================================="
            )

            if model is None:
                over_time_budget = True
                self.logger.info(
                    "Time budged exceeded in create_model(), breaking loop"
                )
                break

            runtime_end = time.time()
            runtime = np.array(runtime_end - runtime_start).round(2)

            self.logger.info("Creating metrics dataframe")
            if cross_validation:
                # cutoff only present in time series and when cv = True
                if "cutoff" in model_results.columns:
                    model_results.drop("cutoff", axis=1, errors="ignore")
                compare_models_ = pd.DataFrame(
                    model_results.loc[
                        self._get_return_train_score_indices_for_logging(
                            return_train_score=False
                        )
                    ]
                ).T.reset_index(drop=True)
            else:
                compare_models_ = pd.DataFrame(model_results.iloc[0]).T
            compare_models_.insert(
                len(compare_models_.columns), "TT (Sec)", model_fit_time
            )
            compare_models_.insert(0, "Model", model_name)
            compare_models_.insert(0, "Object", [model])
            compare_models_.insert(0, "runtime", runtime)
            compare_models_.index = [model_id]
            if master_display is None:
                master_display = compare_models_
            else:
                master_display = pd.concat(
                    [master_display, compare_models_], ignore_index=False
                )
            master_display = master_display.round(round)
            if self._ml_usecase != MLUsecase.TIME_SERIES:
                master_display = master_display.sort_values(
                    by=sort, ascending=sort_ascending
                )
            else:
                master_display = master_display.sort_values(
                    by=sort.upper(), ascending=sort_ascending
                )

            master_display_ = master_display.drop(
                results_columns_to_ignore, axis=1, errors="ignore"
            ).style.format(precision=round)
            master_display_ = master_display_.set_properties(**{"text-align": "left"})
            master_display_ = master_display_.set_table_styles(
                [dict(selector="th", props=[("text-align", "left")])]
            )

            if display.can_update_text:
                display.display(master_display_, final_display=False)

        display.move_progress()

        compare_models_ = self._highlight_models(master_display_)

        display.update_monitor(1, "Compiling Final Models")

        display.move_progress()

        sorted_models = []

        if master_display is not None:
            clamped_n_select = min(len(master_display), abs(n_select))
            if n_select < 0:
                n_select_range = range(
                    len(master_display) - clamped_n_select, len(master_display)
                )
            else:
                n_select_range = range(0, clamped_n_select)

            if self.logging_param:
                self.logging_param.log_model_comparison(
                    master_display, "compare_models"
                )

            for index, row in enumerate(master_display.iterrows()):
                _, row = row
                model = row["Object"]

                results = row.to_frame().T.drop(
                    ["Object", "Model", "runtime", "TT (Sec)"], errors="ignore", axis=1
                )

                avgs_dict_log = {k: v for k, v in results.iloc[0].items()}

                full_logging = False

                if index in n_select_range:
                    display.update_monitor(2, self._get_model_name(model))
                    create_model_args = dict(
                        estimator=model,
                        system=False,
                        verbose=False,
                        fold=fold,
                        round=round,
                        cross_validation=False,
                        predict=False,
                        # fit_kwargs=fit_kwargs,
                        # Changed by sverhoeven to make fit_kwargs nested
                        fit_kwargs=fit_kwargs.get(self._get_model_name(model), {}),
                        groups=groups,
                        probability_threshold=probability_threshold,
                    )
                    if errors == "raise":
                        model, model_fit_time = self._create_model(**create_model_args)
                        sorted_models.append(model)
                    else:
                        try:
                            model, model_fit_time = self._create_model(
                                **create_model_args
                            )
                            sorted_models.append(model)
                            assert (
                                np.sum(
                                    model_results.drop(
                                        results_columns_to_ignore,
                                        axis=1,
                                        errors="ignore",
                                    ).iloc[0]
                                )
                                != 0.0
                            )
                        except Exception:
                            self.logger.error(
                                f"create_model() for {model} raised an exception or returned all 0.0:"
                            )
                            self.logger.error(traceback.format_exc())
                            model = None
                            display.move_progress()
                            continue
                    display.move_progress()
                    full_logging = True

                if self.logging_param and cross_validation and model is not None:
                    self._log_model(
                        model=model,
                        model_results=results,
                        score_dict=avgs_dict_log,
                        source="compare_models",
                        runtime=row["runtime"],
                        model_fit_time=row["TT (Sec)"],
                        pipeline=self.pipeline,
                        log_plots=self.log_plots_param if full_logging else [],
                        log_holdout=full_logging,
                        URI=URI,
                        display=display,
                        experiment_custom_tags=experiment_custom_tags,
                    )

        if len(sorted_models) == 1:
            sorted_models = sorted_models[0]

        display.display(compare_models_, final_display=True)

        pd.reset_option("display.max_columns")

        # store in display container
        self._display_container.append(compare_models_.data)

        self.logger.info(
            f"_master_model_container: {len(self._master_model_container)}"
        )
        self.logger.info(f"_display_container: {len(self._display_container)}")

        self.logger.info(str(sorted_models))
        self.logger.info(
            "compare_models() successfully completed......................................"
        )

        return sorted_models
