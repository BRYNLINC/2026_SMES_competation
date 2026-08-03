from ProcessHub.bci_competition.task.BCICompetitionTaskFinal import BCICompetitionTaskFinal


class BCICompetitionTaskPreliminary(BCICompetitionTaskFinal):
    """
    历史兼容壳。

    正式决赛逻辑已迁移到 BCICompetitionTaskFinal。
    本类仅用于兼容仍然引用旧类名的调试入口和历史文档。
    """

    @staticmethod
    def _BCICompetitionTaskPreliminary__build_final_score_result(score_context: dict) -> dict:
        return BCICompetitionTaskFinal._BCICompetitionTaskFinal__build_final_score_result(score_context)
