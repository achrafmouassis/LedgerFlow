import importlib.util
from pathlib import Path
import unittest
from copy import deepcopy

spec = importlib.util.spec_from_file_location('demo_data', Path(__file__).parents[1] / 'scripts/demo_data.py')
demo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(demo)


class DemoPlanTests(unittest.TestCase):
    def test_plan_reproducible_and_balanced(self):
        plan = demo.make_plan('unit-demo', 120, 6000)
        self.assertEqual(plan, demo.make_plan('unit-demo', 120, 6000))
        self.assertEqual(6000, len(plan['operations']))
        self.assertTrue(all(n >= 0 for n in plan['expected_balance_cents']))
        self.assertEqual({'MAD', 'EUR', 'USD'}, {a['currency'] for a in plan['accounts']})
        self.assertEqual({'DEPOSIT', 'WITHDRAWAL', 'TRANSFER'}, {op['kind'] for op in plan['operations']})
        self.assertTrue(all(a['owner'].startswith('DEMO |') for a in plan['accounts']))

    def test_detects_wrong_balance(self):
        plan = demo.make_plan('unit-demo', 12, 30)
        plan['expected_balance_cents'][0] += 1
        with self.assertRaises(ValueError):
            demo.validate_plan(plan)

    def test_detects_duplicate_operation(self):
        plan = demo.make_plan('unit-demo', 12, 30)
        plan['operations'].append(deepcopy(plan['operations'][0]))
        with self.assertRaises(ValueError):
            demo.validate_plan(plan)

    def test_detects_cross_currency_transfer(self):
        plan = demo.make_plan('unit-demo', 120, 6000)
        transfer = next(o for o in plan['operations'] if o['kind'] == 'TRANSFER')
        transfer['credit'] = next(i for i, a in enumerate(plan['accounts']) if a['currency'] != transfer['currency'])
        with self.assertRaises(ValueError):
            demo.validate_plan(plan)

    def test_money_exact(self):
        self.assertEqual('0.01', demo.money(1))
        self.assertEqual('123.45', demo.money(12345))


if __name__ == '__main__':
    unittest.main()
