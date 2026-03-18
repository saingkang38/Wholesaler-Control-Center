class BaseCollector:
    """
    모든 도매처 collector가 상속해야 하는 기본 클래스.
    각 도매처 collector는 run() 메서드를 반드시 구현해야 한다.
    """
    wholesaler_code = None

    def run(self) -> dict:
        """
        수집 실행. 반드시 아래 형태의 dict를 반환해야 한다.
        {
            "success": True/False,
            "total_items": int,
            "total_pages": int,
            "success_count": int,
            "fail_count": int,
            "error_summary": str or None,
            "items": []
        }
        """
        raise NotImplementedError(f"{self.__class__.__name__} run() 미구현")