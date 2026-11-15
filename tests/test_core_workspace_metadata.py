import ast
import hashlib
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]

class CoreWorkspaceMetadataTests(unittest.TestCase):
    def test_every_core_exports_optional_data_only_workspace_metadata(self):
        manifest=json.loads((ROOT/'core_manifest.json').read_text())
        self.assertEqual(len(manifest['cores']),12)
        for row in manifest['cores']:
            with self.subTest(core=row['id']):
                path=ROOT/'cores'/f"{row['module_key']}.py"
                tree=ast.parse(path.read_text())
                assignments={target.id:node for node in tree.body if isinstance(node,ast.Assign) for target in node.targets if isinstance(target,ast.Name)}
                metadata=ast.literal_eval(assignments['CORE_WORKSPACE'].value)
                self.assertEqual(metadata['schema'],1)
                self.assertTrue(metadata['description'])
                self.assertTrue(metadata['icon'])
                self.assertTrue(metadata['screens'])
                allowed_layouts={'tasks','rules','events','timeline','memories','people','feeds','nodes','devices','accounts','sources','picks','weather','forecast','trends','network','agenda','insights','media','behavior','music','queue'}
                for screen in metadata['screens'].values():
                    self.assertIn(screen['layout'],allowed_layouts)
                    self.assertTrue(screen['label'])
                    self.assertTrue(screen['icon'])
                self.assertIsInstance(metadata['preview_fields'],list)
                self.assertTrue(metadata['edit_label'])
                self.assertLess(len(json.dumps(metadata)),32768)
                self.assertEqual(row['sha256'],hashlib.sha256(path.read_bytes()).hexdigest())
                self.assertEqual(row['version'],ast.literal_eval(assignments['__version__'].value))
                for group in metadata.get('field_groups',[]):
                    self.assertTrue(group['label'])
                    self.assertIsInstance(group['terms'],list)
                exports=[n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Subscript) and isinstance(t.value,ast.Name) and t.value.id=='CORE_WEBUI_TAB' for t in n.targets)]
                self.assertTrue(any(isinstance(n.value,ast.Name) and n.value.id=='CORE_WORKSPACE' for n in exports))

if __name__=='__main__':unittest.main()
