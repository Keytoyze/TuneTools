from tunetools import decorator, Parameter


@decorator.main(num_sample=1)
def main(
        alpha: Parameter(default=0.5, domain=[0, 0.3, 0.5]),
        beta: Parameter(default=1, domain=[0, 1, 2]),
        lr: Parameter(default=0.001, domain=[0.01, 0.001, 0.0001]),
        dataset: Parameter(default="d1", domain=["d1", "d2", "d3"]),
        model: Parameter(default="baseline", domain=["baseline", "model1", "model2"]),
        gpu: Parameter(default=0, domain=[0])
):
    print(">>> start: ", alpha, beta, lr, dataset, model)
    import random
    extra = 0
    import time
    time.sleep(1)
    print(gpu)
    if model == 'baseline':
        extra = -0.5
    return {
        "result": alpha + beta + lr + extra + random.random() / 100,
        "result2": dataset + "!"
    }


@decorator.filtering
def filter_func(alpha, beta, lr, dataset, model, gpu):
    if model == 'baseline' and (alpha != 0.5 or beta != 1.5):
        return False
    if model == 'model1' and (alpha != 0.5 or beta != 1.5):
        return False
    return True


@decorator.onfinish
def onfinish(run_count):
    print("finish!!", run_count)
