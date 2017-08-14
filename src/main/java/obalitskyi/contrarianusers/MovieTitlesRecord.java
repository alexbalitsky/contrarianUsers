package obalitskyi.contrarianusers;

import java.io.Serializable;


public class MovieTitlesRecord implements Serializable {
    private Integer year;
    private String title;

    public MovieTitlesRecord(Integer year, String title) {
        this.year = year;
        this.title = title;
    }

    public MovieTitlesRecord() {
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MovieTitlesRecord that = (MovieTitlesRecord) o;

        if (year != null ? !year.equals(that.year) : that.year != null) return false;
        return title != null ? title.equals(that.title) : that.title == null;
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (title != null ? title.hashCode() : 0);
        return result;
    }
}
