import os
import sys
import unittest

sys.path.insert(0, os.getcwd())
import tests.test_ban_rule

suite = unittest.defaultTestLoader.loadTestsFromModule(tests.test_ban_rule)
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
