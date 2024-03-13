"""Luigi task that creates fileSec and physical structure map."""

from siptools.scripts import compile_structmap

from siptools_research.workflowtask import WorkflowTask


class CreateStructMap(WorkflowTask):

    def run(self):
        """Creates structural map and file section.

        Structural map and file section are written to separate METS
        documents: `<sip_creation_path>/structmap.xml` and
        `<sip_creation_path>/filesec.xml`
        """
        compile_structmap.compile_structmap(
            workspace=self.dataset.sip_creation_path,
            structmap_type='Fairdata-physical',
            stdout=False
        )
