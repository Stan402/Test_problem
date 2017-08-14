package ru.geekbrains.big_test;

/**
 * Класс описывает понятие Натурального ключа,
 * который объединяет в себя два поля базы данных depCode и depJob
 * по условиям задачи комбинация этих полей должна быть уникальной для каждой записи
 */
public class NaturalKey {
    private String depCode;
    private String depJob;

    public NaturalKey(String depCode, String depJob) {
        this.depCode = depCode;
        this.depJob = depJob;
    }

    public String getDepCode() {
        return depCode;
    }

    public String getDepJob() {
        return depJob;
    }

    @Override
    public String toString() {
        return "NaturalKey{" +
                "depCode='" + depCode + '\'' +
                ", depJob='" + depJob + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NaturalKey)) return false;

        NaturalKey that = (NaturalKey) o;

        if (getDepCode() != null ? !getDepCode().equals(that.getDepCode()) : that.getDepCode() != null) return false;
        return getDepJob() != null ? getDepJob().equals(that.getDepJob()) : that.getDepJob() == null;
    }

    @Override
    public int hashCode() {
        int result = getDepCode() != null ? getDepCode().hashCode() : 0;
        result = 31 * result + (getDepJob() != null ? getDepJob().hashCode() : 0);
        return result;
    }
}
