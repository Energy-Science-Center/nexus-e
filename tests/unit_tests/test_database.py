from nexus_e.database import DatabaseCopyNamer


class TestDatabase:
    def get_database_copy_name_generator(self) -> DatabaseCopyNamer:
        return DatabaseCopyNamer(user_initials="XX")

    def test_create_database_name_derived_from_original_name(self):
        # Arrange
        original_name = "any_name"
        sut = self.get_database_copy_name_generator()

        # Act
        result = sut.create_copy_name(original_name)

        # Assert
        assert original_name in result
        assert result != original_name

    def test_create_different_database_names(self):
        # Arrange
        original_name = "any_name"
        sut = self.get_database_copy_name_generator()

        # Act
        first_name = sut.create_copy_name(original_name)
        second_name = sut.create_copy_name(original_name)

        # Assert
        assert first_name != second_name

    def test_create_database_name_shorter_than_max_length(self):
        # Arrange
        max_length = 64  # MySQL maximum length for database names
        original_name = "".join("a" for i in range(max_length))
        sut = self.get_database_copy_name_generator()

        # Act
        result = sut.create_copy_name(original_name)

        # Assert
        assert len(result) <= max_length
        assert result != original_name
