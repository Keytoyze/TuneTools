import time
import tunetools as tt

if __name__ == "__main__":
    def test(x):
        print(">>> start: " + str(x))
        print(">>> end")
        import random
        extra = 0
        if x['model'] == 'baseline':
            extra = -0.5
        return {
            "result": x['alpha'] + x['beta'] + x['lr'] + extra + random.random() / 100,
            "result2": x['dataset'] + "!"
        }


    def filter(x):
        if x['model'] == 'baseline' and (x['alpha'] != 0.5 or x['beta'] != 1.5):
            return False
        if x['model'] == 'model1' and (x['alpha'] != 0.5 or x['beta'] != 1.5):
            return False
        return True

    def onfinish(num_count):
        print("finish!!")

    search_spaces = [
        tt.GridSearchSpace("alpha", default=0.5, domain=[0, 0.3, 0.5]),
        tt.GridSearchSpace("beta", default=1.5, domain=[0, 1.3, 1.5]),
        tt.GridSearchSpace("lr", default=0.001, domain=[0.01, 0.001, 0.0001]),
        tt.GridSearchSpace("model", default="baseline", domain=["baseline", "model1", "model2"]),
        tt.GridSearchSpace("dataset", default="d1", domain=["d1", "d2", "d3"]),
        tt.GridSearchSpace("dataset2", default="", domain=["d1", "d2", "d3"]),
    ]

    tt.set_verbose(True)

    # tt.run(obj_function=test, filter_function=filter, num_sample=20, parameters=search_spaces)
    # tt.test(obj_function=test, parameters=search_spaces, values={'alpha': 4})
    tt.test_or_run(obj_function=test, filter_function=filter, num_sample=1, parameters=search_spaces, on_finish_function=onfinish)

